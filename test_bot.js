// Bot test suite — loads bot-engine.js and runs targeted queries
const fs = require('fs');
const vm = require('vm');
let src = fs.readFileSync('/home/marwan/My_Knowledge_Platform/js/bot-engine.js', 'utf8');
// Patch const/let to var so vm exposes them on global
src = src.replace(/^const\s+/gm, 'var ').replace(/^let\s+/gm, 'var ');

// Stub browser globals
global.KNOWLEDGE = []; global.SQL_TOPICS = []; global.PYTHON_MODULES = [];
global.GAPS = []; global.INTERVIEW_QA = [];

vm.runInThisContext(src);

let pass = 0, fail = 0, failures = [];

function test(label, query, expectId) {
  const tokens = botTokenize(query);
  const subject = extractSubject(query);
  const intent = detectIntent(query);
  const scores = BOT_KB.map(e => ({ id: e.id, score: scoreKB(e, tokens, subject, intent) }))
    .filter(x => x.score > 0).sort((a, b) => b.score - a.score);
  const winner = scores.length > 0 ? scores[0] : null;
  const ok = winner && winner.id === expectId;
  if (ok) { pass++; }
  else {
    fail++;
    const top3 = scores.slice(0, 3).map(x => `${x.id}(${x.score})`).join(', ');
    failures.push(`FAIL [${label}]\n  query: "${query}"\n  want:  ${expectId}\n  got:   ${top3 || 'nothing'}`);
  }
}

function testTokens(label, query, expectTokens) {
  const tokens = botTokenize(query);
  const ok = expectTokens.every(t => tokens.includes(t)) && !tokens.includes('ineering');
  if (ok) { pass++; }
  else { fail++; failures.push(`FAIL [${label}]\n  query: "${query}"\n  tokens: [${tokens.join(', ')}]\n  want: ${expectTokens.join(', ')}, no phantom 'ineering'`); }
}

function testBotRespond(label, query, expectContains) {
  // Reset state
  BOT_QUIZ_PENDING = null;
  const resp = botRespond(query);
  const ok = resp.toLowerCase().includes(expectContains.toLowerCase());
  if (ok) { pass++; }
  else { fail++; failures.push(`FAIL [${label}]\n  query: "${query}"\n  want contains: "${expectContains}"\n  got: "${resp.slice(0,200)}..."`); }
}

// ── Token tests ──
testTokens('no phantom ineering', 'explain docker for data engineering', ['docker','data','engineering']);
testTokens('data eng synonym expands', 'data eng basics', ['data','engineering']);
testTokens('failing maps to debug', 'my pipeline is failing', ['debug']);
testTokens('broken maps to debug', 'pipeline is broken', ['debug']);

// ── Core topics ──
test('what is de', 'what is data engineering', 'what-is-de');
test('etl vs elt', 'difference between etl and elt', 'etl-elt');
test('etl vs elt 2', 'etl vs elt', 'etl-elt');
test('etl vs elt different from', 'how is etl different from elt', 'etl-elt');
test('data warehouse', 'what is a data warehouse', 'data-warehouse');
test('data modeling', 'explain data modeling', 'data-modeling');
test('star schema', 'what is star schema', 'star-schema');
test('incremental loading', 'how does incremental loading work', 'incremental-loading');
test('idempotency', 'what is idempotency in data pipelines', 'idempotency');
test('pipeline phases', 'what is the medallion architecture', 'pipeline-phases');
test('acid', 'what is ACID in databases', 'acid');
test('data quality', 'what is data quality', 'data-quality');

// ── SQL ──
test('joins', 'explain sql joins', 'sql-joins');
test('window functions', 'what are window functions', 'window-functions');
test('window rank', 'how does rank work in sql', 'window-functions');
test('ctes', 'what is a CTE', 'ctes');
test('group by having', 'difference between where and having', 'group-by-having');
test('null handling', 'how to handle nulls in sql', 'null-handling');
test('null handling nulls', 'what if data has nulls', 'null-handling');
test('null handling missing', 'how to deal with missing values', 'null-handling');
test('indexes', 'what is a database index', 'indexes');
test('merge upsert', 'how to do upsert in postgres', 'merge-upsert');
test('merge upsert 2', 'what is upsert', 'merge-upsert');
test('query performance', 'how to optimize a slow query', 'query-performance');
test('explain analyze', 'explain analyze postgres', 'query-performance');

// ── Python ──
test('python setup', 'how to set up python for data engineering', 'python-setup');
test('python pandas', 'how to use pandas', 'python-pandas');
test('python postgres', 'connect python to postgres', 'python-postgres');
test('python error handling', 'how to handle errors in python', 'python-error-handling');
test('python error handling 2', 'handle errors in python pipelines', 'python-error-handling');
test('python generators', 'what are python generators', 'python-generators');
test('python decorators', 'what are decorators in python', 'python-decorators');
test('python typing', 'what are type hints in python', 'python-typing');
test('api pagination', 'how to handle api pagination', 'api-pagination');

// ── Tools ──
test('airflow basics', 'what is apache airflow', 'airflow-basics');
test('airflow dag', 'how to write an airflow dag', 'airflow-basics');
test('spark basics', 'what is apache spark', 'spark-basics');
test('spark lazy', 'what is spark lazy evaluation', 'spark-lazy');
test('dbt basics bare', 'what is dbt', 'dbt-basics');
test('dbt basics 2', 'how to use dbt models', 'dbt-basics');
test('dbt advanced', 'what are dbt macros', 'dbt-advanced');
test('docker basics', 'what is docker', 'docker-basics');
test('docker for de', 'explain docker for data engineering', 'docker-basics');
test('kafka', 'what is kafka', 'kafka-basics');
test('git for de', 'how to use git for data engineering', 'git-for-de');

// ── "for DE" context queries — should NOT return what-is-de ──
test('python for de', 'explain python for data engineering', 'python-setup');
test('sql for de', 'explain sql for data engineering', 'sql-joins');
test('airflow for de', 'explain airflow for data engineering', 'airflow-basics');
test('spark for de', 'explain spark for data engineering', 'spark-basics');

// ── Typo queries ──
test('typo spark pysprak', 'explain pysprak', 'spark-basics');
test('typo spark waht', 'waht is spark', 'spark-basics');
test('typo airlfow', 'what is airlfow', 'airflow-basics');
test('typo dokcer', 'what is dokcer', 'docker-basics');

// ── Debugging ──
test('debugging pipelines', 'my pipeline is failing', 'debugging-pipelines');
test('debugging broken', 'pipeline is broken what do i do', 'debugging-pipelines');
test('debugging crash', 'dag crashed in airflow', 'debugging-pipelines');

// ── Advanced topics ──
test('schema evolution', 'what is schema evolution', 'schema-evolution');
test('data contracts', 'what are data contracts', 'data-contracts');
test('observability', 'what is data observability', 'observability');
test('cloud storage', 'how to use s3 for data engineering', 'cloud-storage');
test('delta iceberg', 'what is delta lake', 'delta-iceberg');
test('cdc debezium', 'what is change data capture', 'cdc-debezium');
test('cost optimization', 'how to reduce warehouse costs', 'cost-optimization');
test('testing pipelines', 'how to test a data pipeline', 'testing-pipelines');
test('career de', 'how to get a data engineering job', 'career-de');

// ── botRespond integration ──
testBotRespond('greeting', 'hello', 'ask');
testBotRespond('help', 'help', 'SQL');
testBotRespond('quiz start', 'quiz me', 'Quiz time');
testBotRespond('answer outside quiz', 'answer', 'No active quiz');

// ── Summary ──
console.log(`\n${'='.repeat(50)}`);
console.log(`RESULTS: ${pass}/${pass+fail} passed`);
if (failures.length > 0) {
  console.log(`\nFAILURES:\n`);
  failures.forEach(f => console.log(f + '\n'));
}
