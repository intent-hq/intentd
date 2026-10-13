#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import yaml

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('validation',ROOT/'scripts/validate-macos-supervisor.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)


class ValidationTests(unittest.TestCase):
    def test_actual_supervisor_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp);log=v.control.Log(directory/'events.jsonl')
            try:
                results=[v.run_case(name,directory,log,time.monotonic()+60) for name in v.CASES]
            finally:log.close()
            self.assertEqual(len(results),8)
            self.assertTrue(all(r['validation_passed'] and r['harness_cleanup_confirmed'] for r in results))
            self.assertEqual(v.d.UNCONFIRMED,[])
            print(json.dumps({'local_actual_supervisor_cases':[{'case':r['case'],'validation_passed':r['validation_passed'],'harness_cleanup_confirmed':r['harness_cleanup_confirmed'],'supervisor_cleaned':r['supervisor']['cleaned'],'cleanup_path':r['supervisor'].get('cleanup_path'),'signals':r['signals']} for r in results]}))
            events=[json.loads(line) for line in (directory/'events.jsonl').read_text().splitlines()]
            self.assertEqual(sum(e['stage']=='owned_spawn' for e in events),8)
            self.assertTrue(any(e.get('injected') and e['actual']!=e['returned'] for e in events))

    def test_invalid_dispatch_retains_failure_without_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)/'fresh'
            with patch.dict(os.environ,dict(SUPERVISOR_VALIDATION_EVIDENCE=str(directory),GITHUB_RUN_ATTEMPT='2')),patch.object(v,'run_case') as run:
                self.assertEqual(v.main(),1)
            run.assert_not_called()
            result=json.loads((directory/'validation.json').read_text())
            self.assertFalse(result['success']);self.assertEqual(result['cases'],[])
            self.assertFalse((directory/'invoked').exists())
            self.assertTrue((directory/'manifest.json').exists())

    def test_validation_rejects_false_cleanup_and_extra_signals(self):
        record=dict(cleaned=True,exit_code=0,timeout=False,cleanup_path='already_quiescent',quiescent_group_observations=[[],[]])
        with self.assertRaises(RuntimeError):v.validate_record('normal_exit',record,[v.signal.SIGSTOP],None)
        with self.assertRaises(RuntimeError):v.validate_record('missing_owner',record,[],{'message':'missing'})

    def test_exclusive_native_route_and_unchanged_rust(self):
        import subprocess
        workflow=yaml.safe_load((ROOT/'.github/workflows/m1mac-smoke.yml').read_text())
        jobs=workflow['jobs'];job=jobs['supervisor-validation']
        self.assertEqual(job['if'],'${{ inputs.supervisor_validation && !inputs.supervisor_control && !inputs.context_boundary_diagnostic }}')
        for other in ('smoke','supervisor-control','context-boundary-diagnostic'):
            self.assertIn('!inputs.supervisor_validation',jobs[other]['if'])
        self.assertEqual(job['runs-on'],'macos-15');self.assertEqual(job['timeout-minutes'],5)
        step=job['steps'][1];upload=job['steps'][2]
        self.assertEqual(step['run'],'python3 -I -B scripts/validate-macos-supervisor.py')
        self.assertEqual(step['env']['SUPERVISOR_VALIDATION_EVIDENCE'],upload['with']['path'])
        self.assertEqual(upload['if'],'${{ always() }}')
        self.assertNotIn('runner.temp',str(job['env']))
        self.assertNotIn('cargo',str(job));self.assertNotIn('rust-toolchain',str(job))
        changed=subprocess.check_output(['git','diff','59f230c5','--name-only'],cwd=ROOT,text=True).splitlines()
        self.assertFalse(any(p.startswith('crates/') or p.endswith('/ci.yml') for p in changed))
        source=(ROOT/'scripts/validate-macos-supervisor.py').read_text()
        self.assertIn('d.supervised(',source);self.assertNotIn('d.main(',source)
        self.assertIn('signal.setitimer(signal.ITIMER_REAL, 110)',source)
        self.assertIn('deadline = started+120',source)


if __name__=='__main__':unittest.main(verbosity=2)
