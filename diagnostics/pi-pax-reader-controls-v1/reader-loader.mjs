import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
const hostRequire=createRequire(import.meta.url);
const manifestSha='4db8adaba849fbbf2f7bfec0d7ebe025f3e3170f55999a136b9089b8e08ad1d2';
const builtins=new Set(['assert','buffer','events','node:events','node:path','node:stream','node:string_decoder','zlib']);
const digest=b=>createHash('sha256').update(b).digest('hex');
function guard(value,id){if(!value)throw new Error('reader_loader_'+id);}
export function createReaderLoader(packet,readBytes=n=>fs.readFileSync(path.join(packet,n))){
 const bytes=readBytes('reader-manifest.json');guard(Buffer.isBuffer(bytes)&&bytes.length<=1048576&&digest(bytes)===manifestSha,'manifest_hash');
 const manifest=JSON.parse(bytes.toString('utf8'));
 guard(manifest.revision==='955266bfdd854cd280dffd47548673914484e4c0'&&Object.keys(manifest.modules).length===11,'manifest_shape');
 guard(JSON.stringify([...builtins].sort())===JSON.stringify([...manifest.builtinAllowlist].sort()),'builtins');
 guard(process.platform==='win32'&&process.version==='v24.21.0'&&!Object.hasOwn(process.env,'TESTING_TAR_FAKE_PLATFORM'),'platform');
 const sources=new Map(),cache=new Map(),ledger=[];let total=0;
 // Validate every source before evaluating any module; no lazy hash failure after partial admission.
 for(const [id,record]of Object.entries(manifest.modules)){
  guard(/^deps\/npm\/node_modules\/[a-z0-9./-]+\.js$/.test(id)&&!id.split('/').includes('..')&&/^reader-[0-9]{2}\.cjs$/.test(record.file),'module_name');
  const source=readBytes(record.file);guard(Buffer.isBuffer(source),'source_hash');total+=source.length;
  guard(Buffer.isBuffer(source)&&source.length===record.bytes&&source.length<=1048576&&total<=33554432&&digest(source)===record.sha256,'source_hash');
  const text=source.toString('utf8');guard(Buffer.from(text).equals(source),'source_encoding');sources.set(id,text);
 }
 function resolve(from,spec){
  guard(Object.hasOwn(manifest.modules,from)&&typeof spec==='string'&&Object.hasOwn(manifest.modules[from].requires,spec),'specifier');
  const target=manifest.modules[from].requires[spec];
  if(Object.hasOwn(target,'builtin')){guard(builtins.has(target.builtin)&&target.builtin===spec,'builtin');return target;}
  guard(Object.keys(target).length===1&&Object.hasOwn(manifest.modules,target.module),'target');return target;
 }
 function load(id){
  guard(Object.hasOwn(manifest.modules,id),'module');
  if(cache.has(id))return cache.get(id).exports;
  const module={exports:{},loaded:false,id};cache.set(id,module);ledger.push(id);
  try{
   const require=spec=>{const target=resolve(id,spec);return Object.hasOwn(target,'builtin')?hostRequire(target.builtin):load(target.module);};
   const fn=new Function('exports','require','module','__filename','__dirname',sources.get(id));
   fn.call(module.exports,module.exports,require,module,id,path.posix.dirname(id));module.loaded=true;return module.exports;
  }catch(error){cache.delete(id);throw error;}
 }
 return Object.freeze({load,resolve,entry:manifest.entry,loaded:()=>[...ledger],sourceHashes:()=>Object.fromEntries(Object.entries(manifest.modules).map(([id,m])=>[id,m.sha256]))});
}
export async function readNativeTar(Parser,input){
 guard(Buffer.isBuffer(input)&&input.length<=131072,'fixture_cap');
 return await new Promise(resolve=>{
  const rows=[];let done=false,total=0,entries=0,ended=0,closed=false;
  const parser=new Parser({strict:true,maxMetaEntrySize:1048576});
  const finish=(ok,code)=>{if(done)return;done=true;clearTimeout(timer);resolve({ok,code,rows,entries,ended,closed,total});};
  const stop=code=>{finish(false,code);try{parser.abort(new Error('reader_fixture_abort'));}catch{}};
  const timer=setTimeout(()=>stop('timeout'),1000);
  parser.on('error',()=>stop('error'));parser.on('warn',()=>stop('warn'));
  parser.on('close',()=>{closed=true;});
  parser.on('entry',entry=>{
   if(done){entry.resume();return;}
   if(++entries>32){entry.resume();stop('entry_cap');return;}
   const row={path:entry.path,type:entry.type,size:entry.size,bytes:0,sha256:null};rows.push(row);const hash=createHash('sha256');
   entry.on('error',()=>stop('entry_error'));
   entry.on('data',chunk=>{if(done)return;total+=chunk.length;row.bytes+=chunk.length;if(total>65536){stop('body_cap');return;}hash.update(chunk);});
   entry.on('end',()=>{if(done)return;row.sha256=hash.digest('hex');ended++;});
   entry.resume();
  });
  parser.on('end',()=>{queueMicrotask(()=>{if(!done)finish(ended===entries,'end');});});
  try{parser.end(input);}catch{stop('throw');}
 });
}
