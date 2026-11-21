const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const context={};vm.createContext(context);vm.runInContext(fs.readFileSync(`${__dirname}/../frontend/rating-workspace.js`,'utf8'),context);
let result=context.rwScenario({5:17,1:2},0,{});
assert.equal(result.total,19);assert.equal(result.mean,87/19);assert.equal(result.need,4);
result=context.rwScenario({5:17,1:2},4,{});assert(result.mean>=4.65);
assert(context.rwScenario({5:17,1:2},3,{}).mean<4.65);
result=context.rwScenario({5:17,1:2},0,{1:500});assert.equal(result.mean,5);assert.equal(result.total,17);assert.equal(result.need,null);
assert.equal(context.rwScenario({},0,{}).mean,null);
assert.equal(context.rwScenario({5:17,1:2},0,{1:1}).total,18);
assert.equal(context.rwScenario({5:17,1:2},0,{1:1}).shown,4.8);
console.log('Rating scenarios, removal bounds and next-step thresholds: OK');
const elements={};
context.document={getElementById:id=>elements[id] ||= {value:'0',checked:false}};
context.RW_FIXTURE={products:[
 {nm_id:'1',article:'A',stars:{1:2,2:0,3:0,4:0,5:10},report:{stars:{1:2,2:0,3:0,4:0,5:10},total:12,scenario_usable:true,period_from:'2026-11-01',period_to:'2026-11-20'}},
 {nm_id:'2',article:'B',stars:{1:-1,2:0,3:0,4:0,5:0},report:{stars:{1:-1,2:0,3:0,4:0,5:0},total:-1,scenario_usable:false,period_from:'2026-11-01',period_to:'2026-11-20'}}
]};
elements.rwSearch={value:''};
vm.runInContext("RW=RW_FIXTURE;RW_SELECTED=new Set(['1','2']);drawRatingWorkspace()",context);
assert(elements.rwCards.innerHTML.includes('4,63636')); // (10*5 + 2 - 1)/(12 - 1)
assert(!elements.rwAdd5.disabled);
assert(elements.rwReportInfo.textContent.includes('учтены со знаком минус'));
vm.runInContext("RW_SELECTED=new Set(['2']);drawRatingWorkspace()",context);
assert(elements.rwAdd5.disabled);
assert(elements.rwForecast.textContent.includes('прогноз недоступен'));
console.log('Signed WB corrections are aggregated once, with invalid negative totals guarded: OK');
