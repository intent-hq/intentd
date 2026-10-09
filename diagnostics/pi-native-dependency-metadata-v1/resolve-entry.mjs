// Resolves only, never imports/executes dependency entry points.
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const fixture=process.argv[2]==='fixture';assert(process.argv.length===(fixture?3:2));
const names=fixture?['conditional-export-probe']:['@agentclientprotocol/sdk','cross-spawn'];
const require=createRequire(import.meta.url);
console.log(JSON.stringify(names.map(name=>({name,importPath:fileURLToPath(import.meta.resolve(name)),requirePath:require.resolve(name)}))));
