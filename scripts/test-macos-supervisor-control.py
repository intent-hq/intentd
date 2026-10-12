#!/usr/bin/env python3
"""Offline controls; never dispatch native work or run Rust."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('control', ROOT/'scripts/macos-supervisor-control.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class Controls(unittest.TestCase):
    def test_native_and_dispatch_guards(self):
        env = dict(GITHUB_RUN_ATTEMPT='1', GITHUB_EVENT_NAME='workflow_dispatch',
                   EXPECTED_SOURCE_SHA='a'*40, GITHUB_SHA='a'*40)
        c.guard(env, 'a'*40, 'Darwin', 'arm64')
        for key, value in [('GITHUB_RUN_ATTEMPT', '2'), ('GITHUB_EVENT_NAME', 'push'),
                           ('EXPECTED_SOURCE_SHA', 'b'*40), ('GITHUB_SHA', 'b'*40)]:
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                c.guard(dict(env, **{key:value}), 'a'*40, 'Darwin', 'arm64')
        for system, machine in [('Linux','aarch64'), ('Darwin','x86_64')]:
            with self.assertRaises(RuntimeError):
                c.guard(env,'a'*40,system,machine)

    def test_evidence_is_fresh_and_external(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'repo'; root.mkdir()
            with self.assertRaises(RuntimeError): c.prepare_directory(root/'logs', root)
            with self.assertRaises(RuntimeError): c.prepare_directory(Path('relative'), root)
            out=Path(tmp)/'evidence'; c.prepare_directory(out,root)
            with self.assertRaises(FileExistsError): c.prepare_directory(out,root)

    def test_operation_errno_and_stage_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=c.Log(Path(tmp)/'events.jsonl')
            with patch.object(os,'killpg',side_effect=PermissionError(1,'Operation not permitted')):
                result=c.operation(log,'normal_exit','freeze.killpg.SIGSTOP',lambda:os.killpg(123,signal.SIGSTOP))
            self.assertFalse(result['ok']); self.assertEqual(result['errno'],1)
            log.close()
            event=json.loads((Path(tmp)/'events.jsonl').read_text())
            self.assertEqual(event['stage'],'freeze.killpg.SIGSTOP')
            self.assertEqual(event['error_type'],'PermissionError')

    def test_ps_parser_and_state_requirements(self):
        self.assertEqual(c.parse_ps(' 12 3 12 Z\n 13 12 12 T\n 15 2 15 S\n',12),
                         [dict(pid=12,ppid=3,pgid=12,state='Z'),dict(pid=13,ppid=12,pgid=12,state='T')])
        with self.assertRaises(ValueError): c.parse_ps('partial',12)
        self.assertFalse(c.matches([],123,'exited'))
        self.assertTrue(c.matches([dict(pid=123,state='Z')],123,'exited'))
        self.assertFalse(c.matches([dict(pid=123,state='S')],123,'exited'))

    def test_no_signal_after_reap(self):
        child=c.OwnedChild(None)
        child.owned=False
        with patch.object(os,'killpg') as call, self.assertRaises(RuntimeError):
            child.send(signal.SIGSTOP)
        call.assert_not_called()

    def test_real_small_children_and_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=c.Log(Path(tmp)/'events.jsonl')
            results=[c.run_arm(name,log,time.monotonic()+30) for name in ('normal_exit','live_child')]
            log.close()
            for result in results:
                self.assertTrue(result['state_confirmed'],result)
                self.assertTrue(result['cleanup_confirmed'],result)
                self.assertTrue(result['eof'],result)
            self.assertEqual(results[0]['exit_code'],0)
            events=[json.loads(x) for x in (Path(tmp)/'events.jsonl').read_text().splitlines()]
            self.assertTrue(any(e['stage']=='freeze.killpg.SIGSTOP' for e in events))
            self.assertTrue(any(e['stage']=='cleanup.before_reap' for e in events))

    def test_group_errors_still_cleanup_direct_owned_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=c.Log(Path(tmp)/'events.jsonl')
            with patch.object(os,'killpg',side_effect=PermissionError(1,'denied')):
                result=c.run_arm('live_child',log,time.monotonic()+30)
            log.close()
            self.assertTrue(result['cleanup_confirmed'],result)
            events=[json.loads(x) for x in (Path(tmp)/'events.jsonl').read_text().splitlines()]
            self.assertTrue(any(e['stage']=='cleanup.kill.SIGKILL' and e['ok'] for e in events))

    def test_no_spawn_after_expired_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=c.Log(Path(tmp)/'events.jsonl')
            with patch.object(c.subprocess,'Popen') as spawn, self.assertRaises(TimeoutError):
                c.run_arm('normal_exit',log,time.monotonic()-1)
            log.close(); spawn.assert_not_called()

    def test_cleanup_exception_retains_owned_anchor_and_reports_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=c.Log(Path(tmp)/'events.jsonl')
            count=len(c.UNCONFIRMED)
            try:
                with patch.object(c,'cleanup',side_effect=OSError(1,'cleanup observation denied')):
                    result=c.run_arm('normal_exit',log,time.monotonic()+30)
                self.assertFalse(result['cleanup_confirmed'])
                self.assertEqual(result['cleanup_error']['errno'],1)
                self.assertEqual(len(c.UNCONFIRMED),count+1)
            finally:
                for child in c.UNCONFIRMED[count:]:
                    try: child.send(signal.SIGCONT,group=False)
                    except ProcessLookupError: pass
                    try: child.send(signal.SIGKILL,group=False)
                    except ProcessLookupError: pass
                    child.process.wait(timeout=2)
                    child.owned=False
                del c.UNCONFIRMED[count:]
                log.close()

    def test_no_build_or_boundary_import_and_budget(self):
        source=(ROOT/'scripts/macos-supervisor-control.py').read_text()
        self.assertEqual(c.SECONDS,120)
        self.assertNotIn('context-boundary-diagnostic',source)
        self.assertNotIn("['cargo'",source)
        self.assertIn('signal.setitimer(signal.ITIMER_REAL, SECONDS-10)',source)
        self.assertIn("for name in ('normal_exit', 'live_child'):",source)

    def test_frozen_diagnostic_is_byte_identical(self):
        import subprocess
        for name in ['scripts/context-boundary-diagnostic.py', '.github/workflows/ci.yml']:
            old=subprocess.check_output(['git','show','89d41513:'+name],cwd=ROOT)
            self.assertEqual((ROOT/name).read_bytes(),old)

    def test_workflow_route_is_separate_and_bounded(self):
        workflow=yaml.safe_load((ROOT/'.github/workflows/m1mac-smoke.yml').read_text())
        jobs=workflow['jobs']
        self.assertEqual(jobs['smoke']['if'],'${{ !inputs.context_boundary_diagnostic && !inputs.supervisor_control }}')
        self.assertEqual(jobs['context-boundary-diagnostic']['if'],'${{ inputs.context_boundary_diagnostic && !inputs.supervisor_control }}')
        job=jobs['supervisor-control']
        self.assertEqual(job['if'],'${{ inputs.supervisor_control && !inputs.context_boundary_diagnostic }}')
        self.assertEqual(job['runs-on'],'macos-15')
        self.assertLessEqual(job['timeout-minutes'],5)
        execute=[s for s in job['steps'] if s.get('run')][0]
        self.assertEqual(execute['run'],'python3 -I -B scripts/macos-supervisor-control.py')
        upload=job['steps'][-1]
        self.assertEqual(upload['if'],'${{ always() }}')
        self.assertEqual(upload['with']['path'],execute['env']['SUPERVISOR_CONTROL_EVIDENCE'])
        self.assertNotIn('runner.temp',str(job.get('env',{})))
        text=json.dumps(job)
        self.assertNotIn('cargo',text); self.assertNotIn('rust-toolchain',text)
        inputs=workflow.get('on',workflow.get(True))['workflow_dispatch']['inputs']
        self.assertFalse(inputs['supervisor_control']['default'])


if __name__=='__main__': unittest.main(verbosity=2)
