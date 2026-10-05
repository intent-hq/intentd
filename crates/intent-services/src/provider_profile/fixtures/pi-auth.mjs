// Synthetic credential equality only. Never print auth, invoke helpers or call a model.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const [runtime, directory, input] = process.argv.slice(2);
const expected = JSON.parse(fs.readFileSync(input));
try {
  if (JSON.parse(fs.readFileSync(path.join(runtime, 'package.json'))).version !== '0.81.0') process.exit(2);
  const { ModelRuntime } = await import(pathToFileURL(path.join(runtime, 'dist/core/model-runtime.js')));
  const models = await ModelRuntime.create({modelsPath:path.join(directory,'models.json'), authPath:path.join(directory,'auth.json'),allowModelNetwork:false});
  if (expected.absent) process.exit((await models.getAvailable('fixture')).length === 0 ? 0 : 1);
  const actual = await models.getAuth('fixture');
  process.exit(actual?.auth.apiKey === expected.apiKey ? 0 : 1);
} catch { process.exit(1); }
