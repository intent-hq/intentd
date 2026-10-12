#!/usr/bin/env python3
"""Offline controls/containment tests: never builds or invokes the boundary fixture."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('diagnostic', ROOT/'scripts/context-boundary-diagnostic.py')
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


class DiagnosticTests(unittest.TestCase):
    def test_dispatch_guards(self):
        sha = 'a'*40
        env = dict(GITHUB_RUN_ATTEMPT='1', EXPECTED_SOURCE_SHA=sha, GITHUB_SHA=sha)
        d.check_guard(env,sha,'Darwin','arm64')
        for changes in [dict(GITHUB_RUN_ATTEMPT='2'),dict(EXPECTED_SOURCE_SHA=''),dict(GITHUB_SHA='b'*40)]:
            with self.assertRaises(RuntimeError):
                d.check_guard(dict(env,**changes),sha,'Darwin','arm64')
        for system,machine in [('Linux','arm64'),('Darwin','x86_64')]:
            with self.assertRaises(RuntimeError): d.check_guard(env,sha,system,machine)

    def test_invalid_dispatch_preserves_zero_invocations_and_exclusive_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)/'fresh'
            env=dict(os.environ,CONTEXT_BOUNDARY_EVIDENCE=str(directory),GITHUB_RUN_ATTEMPT='2')
            with patch.dict(os.environ,env,clear=True), patch.object(d.platform,'system',return_value='Darwin'), patch.object(d.platform,'machine',return_value='arm64'):
                self.assertEqual(d.main(),1)
                self.assertEqual(json.loads((directory/'diagnostic.json').read_text())['invocations'],0)
                with self.assertRaises(FileExistsError):d.main()

    def test_compiler_metadata_selection_rejects_missing_duplicate(self):
        r=dict(reason='compiler-artifact',target=dict(name='intent_services'),profile=dict(test=True,opt_level='0'),features=[],executable='/actual')
        self.assertEqual(d.compiler_artifact(json.dumps(r)),r)
        for text in ['',json.dumps(r)+'\n'+json.dumps(r)]:
            with self.assertRaises(RuntimeError):d.compiler_artifact(text)

    def test_executed_binary_must_match_compiler_artifact(self):
        d.require_executed_binary('Running unittests src/lib.rs (target/debug/test-a)\n',Path('target/debug/test-a'))
        for output in ['', 'Running unittests src/lib.rs (target/debug/test-b)\n']:
            with self.assertRaisesRegex(RuntimeError,'executed path'):
                d.require_executed_binary(output,Path('target/debug/test-a'))

    def test_partial_logs_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'owners'
            p.write_text('{"event":')
            with self.assertRaises(RuntimeError):d.owner_cleanup(p)

    def test_owner_requires_matching_cleanup_and_scratch_gone(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'owners';scratch=Path(tmp)/'scratch'
            start=dict(event='owner_start',pid=12,root=str(scratch))
            end=dict(event='owner_cleanup',pid=12,root=str(scratch),cleaned=True,scratch_exists=False)
            for events,good in [([start],False),([start,end],True),([start,end,end],False),([start,dict(end,cleaned=False)],False)]:
                p.write_text(''.join(json.dumps(e)+'\n' for e in events));self.assertEqual(d.owner_cleanup(p),good)
            p.write_text(''.join(json.dumps(e)+'\n' for e in [start,end]));scratch.mkdir()
            self.assertFalse(d.owner_cleanup(p))

    def test_normal_exit_preserved_and_group_reaped(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=d.supervised([sys.executable,'-I','-S','-c','print("ok")'],dict(os.environ),Path(tmp)/'out',time.monotonic()+2,cleanup_seconds=2)
            self.assertEqual(r['exit_code'],0,r);self.assertTrue(r['cleaned']);self.assertFalse(r['timeout'])
            self.assertEqual(d.group_members(r['pid']),[])

    def test_timeout_kills_owned_cargo_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=d.supervised([sys.executable,'-I','-S','-c','import time; time.sleep(5)'],dict(os.environ),Path(tmp)/'out',time.monotonic()+.1,cleanup_seconds=2)
            self.assertTrue(r['timeout']);self.assertTrue(r['cleaned']);self.assertNotEqual(r['exit_code'],0)
            self.assertEqual(d.group_members(r['pid']),[])

    def test_separate_worker_group_is_cleaned_by_owner_after_cancel(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);owner=root/'owners';cancel=root/'cancel'
            code='''import os,sys,json,subprocess,time,signal,pathlib
owner,cancel,scratch=map(pathlib.Path,sys.argv[1:])
scratch.mkdir()
def event(name,**fields):
 with owner.open('a') as f: f.write(json.dumps(dict(event=name,pid=os.getpid(),root=str(scratch),**fields))+'\\n')
event('owner_start')
child=subprocess.Popen([sys.executable,'-I','-S','-c','import time;time.sleep(20)'],start_new_session=True)
while not cancel.exists():time.sleep(.01)
os.killpg(child.pid,signal.SIGKILL)
child.wait();scratch.rmdir()
event('owner_cleanup',cleaned=True,scratch_exists=False)
'''
            r=d.supervised([sys.executable,'-I','-S','-c',code,str(owner),str(cancel),str(root/'scratch')],dict(os.environ),root/'out',time.monotonic()+.2,owner,cancel,cleanup_seconds=4)
            self.assertTrue(r['timeout']);self.assertTrue(r['cleaned']);self.assertTrue(d.owner_cleanup(owner))

    def test_unconfirmed_owner_never_signals_worker_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);owner=root/'owners';owner.write_text(json.dumps(dict(event='owner_start',pid=987654,root=str(root/'x')))+'\n')
            sent=[]
            original=d.signal_owned
            def observe(process,sig):sent.append((process.pid,sig));return original(process,sig)
            with patch.object(d,'signal_owned',observe):
                with self.assertRaisesRegex(RuntimeError,'native owner cleanup unconfirmed'):
                    d.supervised([sys.executable,'-I','-S','-c','pass'],dict(os.environ),root/'out',time.monotonic()+2,owner,root/'cancel',cleanup_seconds=2)
            self.assertTrue(sent);self.assertTrue(all(pid!=987654 for pid,sig in sent))
            # Failure deliberately preserves unconfirmed ownership; reap our
            # already exited fake owner to avoid the unit test itself leaking.
            d.UNCONFIRMED.pop().wait(timeout=2)
            self.assertFalse(json.loads((root/'out.process.json').read_text())['cleaned'])

    def test_identity_rejects_zero_workers_before_hashing(self):
        with self.assertRaisesRegex(RuntimeError,'exactly one'):d.validate_trace([],Path('/missing'),'')

    def test_completion_requires_both_real_arms(self):
        with self.assertRaisesRegex(RuntimeError,'both original arms'):d.require_completed_trace([])
        events=[]
        for arm in [63,64]:
            events += [dict(event=e,arm=arm) for e in ['arm_start','assertions_passed','arm_drained']]
            events += [dict(event='span_end',arm=arm,fields=dict(stage='host')) for _ in range(arm)]
            events += [dict(event='span_start',arm=arm,fields=dict(stage='required_admit',records=n)) for n in [2]+[1]*62]
            if arm==63: events += [dict(event='span_start',arm=arm,fields=dict(stage='optional_admit',records=64))]
        events += [dict(event=e,arm=64) for e in ['worker_completed','heartbeat_drop','logger_summary']]
        d.require_completed_trace(events)
        records=next(r for r in events if r.get('fields',{}).get('stage')=='optional_admit')
        records['fields']['records']=63
        with self.assertRaisesRegex(RuntimeError,'real boundary counts'):d.require_completed_trace(events)

    def test_diagnostic_source_keeps_worker_only_probe_and_original_deadline(self):
        consumer=(ROOT/'crates/intent-services/src/repository_admission/read_policy/tests/context_consumer.rs').read_text()
        boundary=consumer.split('async fn context_consumer_ledger_counts',1)[1].split('#[test]',1)[0]
        self.assertLess(boundary.index('isolation::enter'),boundary.index('diagnostic_trace::start'))
        self.assertIn('for count in [63, 64]',boundary);self.assertIn('completion.finish();',boundary)
        tcp=(ROOT/'crates/intent-services/src/repository_context_output/tests.rs').read_text().split('pub(crate) async fn tcp',1)[1].split('#[intent_test_macros',1)[0]
        self.assertIn('Duration::from_secs(15)',tcp)
        self.assertLess(tcp.index('"tcp_read_result"'),tcp.index('result.unwrap().unwrap()'))
        probe=(ROOT/'crates/intent-services/src/repository_context_output/tests/diagnostic_trace.rs').read_text()
        for text in ['MissedTickBehavior::Skip','Duration::from_millis(25)','heartbeat_drop','logger_summary','create_new(true)']:
            self.assertIn(text,probe)

    def test_manual_route_has_no_automatic_trigger_or_normal_ci_edit(self):
        workflow=(ROOT/'.github/workflows/m1mac-smoke.yml').read_text()
        for text in ['default: false','!inputs.context_boundary_diagnostic','runs-on: macos-15','timeout-minutes: 50','persist-credentials: false','always()','contents: read']:
            self.assertIn(text,workflow)
        self.assertNotIn('pull_request:',workflow);self.assertNotIn('push:',workflow)
        old_smoke=subprocess.check_output(['git','show','7f73b4d5:.github/workflows/m1mac-smoke.yml'],cwd=ROOT,text=True).split('  smoke:',1)[1]
        new_smoke=workflow.split('  smoke:',1)[1].split('  context-boundary-diagnostic:',1)[0].replace('    if: ${{ !inputs.context_boundary_diagnostic }}\n','')
        self.assertEqual(old_smoke.strip(),new_smoke.strip())
        old=subprocess.check_output(['git','show','7f73b4d5:.github/workflows/ci.yml'],cwd=ROOT)
        self.assertEqual((ROOT/'.github/workflows/ci.yml').read_bytes(),old)
        script=(ROOT/'scripts/context-boundary-diagnostic.py').read_text()
        self.assertEqual(script.count("supervised(CARGO+[TARGET,'--','--exact','--test-threads=1'"),1)
        self.assertNotIn('--workspace',script);self.assertNotIn('nextest',script)


if __name__=='__main__':unittest.main()
