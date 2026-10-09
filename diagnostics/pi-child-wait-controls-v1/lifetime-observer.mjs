// Bound observation preload, not a replacement child implementation.
import childProcess from 'node:child_process';
import {syncBuiltinESMExports} from 'node:module';
import {writeSync} from 'node:fs';
const fault=new URL(import.meta.url).searchParams.get('fault');
if(fault!==null && fault!=='root-alias')throw Error('observer_fault_selection');
const originalSpawn=childProcess.spawn;
let calls=0;
childProcess.spawn=function(file,args,options){
 if(++calls!==1 || file!==process.execPath || !Array.isArray(args) || args.length!==3 || args[0]!==process.argv[1] || args[1]!=='wait' || args[2]!==process.argv[3] || process.argv[2]!=='retired-parent' || options?.stdio!=='inherit' || Object.keys(options).some(k=>!['stdio','detached'].includes(k)) || (options.detached!==undefined && options.detached!==true))throw Error('observer_spawn_shape');
 const child=Reflect.apply(originalSpawn,this,[file,args,options]);
 // No extra error listener or catch: original spawn/error behavior is not suppressed.
 const report=fault==='root-alias'?process.pid:child.pid;
 if(!Number.isInteger(report) || report<=0)throw Error('observer_child_unavailable');
 writeSync(1,'LIFETIME_CHILD_V1 '+process.pid+' '+report+'\n');
 return child;
};
syncBuiltinESMExports();
