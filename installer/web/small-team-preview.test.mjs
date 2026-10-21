import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-state.mjs';
import {createSimulationScene,simulationViewport} from './simulation-scene.mjs';
import {composeTopology} from './topology-layout.mjs';
import {applyVisualizationLayout} from './visualization-code.mjs';

const plan=count=>({
  agent_classes:Array.from({length:count},(_,i)=>({id:`role${i}`,name:`Distinct context ${i}`,
    description:`Own continuing context ${i}`,instructions:`Keep the evidence and decisions for domain ${i}.`,
    instances:1,members:[],tools:{memory:true,notes:i===0?'write':'read',mcp:[]}})),
  topology:{mode:'manual',entrypoint:'role0:main',edges:count>1?[{a:'role0:main',b:'role1:main'}]:[]},
  connections:[],knowledge:{mode:'none'},flows:[],
});

test('small-team resize preserves every identity, grant and peer without inventing human routing',()=>{
  for(const count of [1,2,3,8]){
    const spec=plan(count),model=createSimulation(spec),before=structuredClone(model);
    const base=createSimulationScene(model,spec);
    const wide=applyVisualizationLayout(base,composeTopology(model),model);
    const narrow=applyVisualizationLayout(base,composeTopology(model,{narrow:true}),model);
    assert.deepEqual(model,before);
    assert.deepEqual(narrow.relationships,wide.relationships);
    assert.deepEqual(narrow.script,wide.script);
    assert.equal(narrow.nodes.filter(n=>n.semanticKind==='agent').length,count);
    assert.ok(narrow.relationships.every(e=>!e.source.startsWith('boundary:') && !e.target.startsWith('boundary:')));
    for(const [scene,width,height] of [[wide,700,350],[narrow,348,348]]){
      const v=simulationViewport(scene,width,height);
      for(const node of scene.nodes.filter(n=>n.kind!=='text')){
        const x=node.x*v.sx+v.ox,y=node.y*v.sy+v.oy;
        assert.ok(x>12 && x<width-12,`${count} ${node.id} x=${x}`);
        assert.ok(y>12 && y<height-12,`${count} ${node.id} y=${y}`);
      }
    }
    assert.notDeepEqual(narrow.nodeIndex['role0:main'],wide.nodeIndex['role0:main']);
  }
});

test('native flow and large-population geometry retain their existing equal-axis projection',()=>{
  const big=plan(49),model=createSimulation(big),scene=createSimulationScene(model,big);
  const v=simulationViewport(scene,700,350);
  assert.equal(v.sx,v.sy);
  const workflow={...scene,population:{count:2},selectedFlowId:'explicit-workflow'};
  assert.equal(simulationViewport(workflow,360,460).sx,simulationViewport(workflow,360,460).sy);
});
