// ---- SCORE CONFIG (max=100) ----
const BASE_SCORE = 72;
const MAX_SCORE  = 100;
const MAX_TOPIC_DELTA = 8;

const GAP_SCORE_WEIGHTS = {
  'scd-types':             4,
  'dbt-tool':              4,
  'advanced-sql':          3,
  'cloud-platforms':       3,
  'wide-narrow-transforms':2,
  'medallion-arch':        1,
  'delta-iceberg':         1,
  'streaming-kafka':       0
};
const CORRECTION_SCORE_WEIGHT = 1;
const MAX_GAP_PTS  = Object.values(GAP_SCORE_WEIGHTS).reduce((a,b)=>a+b,0); // 18
const MAX_CORR_PTS = CORRECTIONS.length * CORRECTION_SCORE_WEIGHT;           // 2
// 72 + 8 + 18 + 2 = 100 ✓

// ---- STATE ----
const CONFIDENCE_LABELS = ['','Barely Know It','Familiar','Comfortable','Confident','Mastered'];
const TOPIC_MAP = {};
KNOWLEDGE.forEach(k => { TOPIC_MAP[k.id] = k; });
const ELI5_VISIBLE  = new Set(); // topicIds where ELI5 panel is open
const GAP_EXPANDED  = new Set(); // gap IDs currently expanded

// ---- NAVIGATE ----
const BUILT_SECTIONS = new Set();

function navigate(sectionId) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const target = document.getElementById(sectionId);
  if (target) { target.classList.add('active'); }
  const navEl = document.querySelector(`[data-nav="${sectionId}"]`);
  if (navEl) navEl.classList.add('active');
  window.scrollTo({ top: 0, behavior: 'smooth' });

  if (!BUILT_SECTIONS.has(sectionId)) {
    BUILT_SECTIONS.add(sectionId);
    switch (sectionId) {
      case 'knowledge':         buildKnowledge(); break;
      case 'corrections':       buildCorrections(); break;
      case 'gaps':              buildGaps(); setupGapFilters(); break;
      case 'connections':       buildConnections(); break;
      case 'interview':         buildReadiness(); break;
      case 'flashcards':        buildFlashcards(); break;
      case 'interview-questions': buildInterviewQuestions(); break;
      case 'snippets':          buildSnippets(); break;
      case 'roadmap':           buildRoadmap(); break;
      case 'sql-fundamentals':  buildSqlFundamentals(); setupSqlFilters(); break;
      case 'python-de':         buildPythonDE(); break;
      case 'setup-guides':      buildSetupGuides(); break;
      case 'projects':          buildProjects(); break;
    }
  }

  if (sectionId === 'interview') setTimeout(animateReadiness, 120);
}

// ---- LOCAL STORAGE ----
function getConfidence(id)           { return parseInt(localStorage.getItem('mk_conf_' + id) || '0'); }
function setConfidence(id, v)        { localStorage.setItem('mk_conf_' + id, v); }
function isGapStudied(id)            { return localStorage.getItem('mk_gap_' + id) === '1'; }
function setGapStudied(id, v)        { localStorage.setItem('mk_gap_' + id, v ? '1' : '0'); }
function isCorrectionReviewed(id)    { return localStorage.getItem('mk_corr_' + id) === '1'; }
function setCorrectionReviewed(id,v) { localStorage.setItem('mk_corr_' + id, v ? '1' : '0'); }
function isQPracticed(topicId, idx)  { return localStorage.getItem(`mk_iq_${topicId}_${idx}`) === '1'; }
function setQPracticed(topicId, idx, v) { localStorage.setItem(`mk_iq_${topicId}_${idx}`, v ? '1' : '0'); }

// ---- STREAK ----
function getStreak() {
  const today = new Date().toDateString();
  let data = {};
  try { data = JSON.parse(localStorage.getItem('mk_streak') || '{}'); } catch(e) {}
  if (!data.lastActive) return { count: 0, todayDone: false };
  const last = new Date(data.lastActive);
  const now  = new Date();
  const diffDays = Math.floor((now - last) / 86400000);
  if (diffDays === 0) return { count: data.count || 1, todayDone: true };
  if (diffDays === 1) return { count: data.count || 1, todayDone: false };
  return { count: 0, todayDone: false }; // streak broken
}

function recordActivity() {
  const streak = getStreak();
  const newCount = streak.todayDone ? streak.count : streak.count + 1;
  localStorage.setItem('mk_streak', JSON.stringify({
    lastActive: new Date().toDateString(),
    count: newCount
  }));
  return newCount;
}

function buildStreakBadge() {
  const streak = getStreak();
  const el = document.getElementById('streak-badge');
  if (!el) return;
  if (streak.count >= 2) {
    el.textContent = `🔥 ${streak.count} day streak`;
    el.style.display = '';
  } else {
    el.style.display = 'none';
  }
}

// ---- SCORE ----
function calcDynamicScore() {
  const rated = KNOWLEDGE.filter(k => getConfidence(k.id) > 0);
  const topicDelta = rated.length > 0
    ? Math.round((rated.reduce((s,k) => s + getConfidence(k.id), 0) / rated.length - 3) * (MAX_TOPIC_DELTA / 2))
    : 0;
  const gapPts  = GAPS.reduce((s,g) => s + (isGapStudied(g.id) ? (GAP_SCORE_WEIGHTS[g.id]||0) : 0), 0);
  const corrPts = CORRECTIONS.reduce((s,c) => s + (isCorrectionReviewed(c.id) ? CORRECTION_SCORE_WEIGHT : 0), 0);
  return Math.min(MAX_SCORE, Math.max(0, Math.round(BASE_SCORE + topicDelta + gapPts + corrPts)));
}

function getScoreBreakdown() {
  const rated = KNOWLEDGE.filter(k => getConfidence(k.id) > 0);
  const topicDelta = rated.length > 0
    ? Math.round((rated.reduce((s,k)=>s+getConfidence(k.id),0)/rated.length-3)*(MAX_TOPIC_DELTA/2))
    : 0;
  const gapPts   = Math.round(GAPS.reduce((s,g)=>s+(isGapStudied(g.id)?GAP_SCORE_WEIGHTS[g.id]||0:0),0)*10)/10;
  const corrPts  = CORRECTIONS.filter(c=>isCorrectionReviewed(c.id)).length * CORRECTION_SCORE_WEIGHT;
  const studied  = GAPS.filter(g=>isGapStudied(g.id)).length;
  const reviewed = CORRECTIONS.filter(c=>isCorrectionReviewed(c.id)).length;
  return { topicDelta, gapPts, corrPts, rated: rated.length, studied, reviewed };
}

// ---- SIDEBAR + BADGES ----
function updateSidebarScore() {
  const score = calcDynamicScore();
  document.getElementById('sidebar-score').textContent = score + '/100';
  const bd = getScoreBreakdown();
  const parts = [];
  if (bd.rated > 0)    parts.push(`${bd.topicDelta>=0?'+':''}${bd.topicDelta} topics`);
  if (bd.studied > 0)  parts.push(`+${bd.gapPts} gaps`);
  if (bd.reviewed > 0) parts.push(`+${bd.corrPts} corrections`);
  const descEl = document.getElementById('sidebar-score-desc');
  if (descEl) descEl.textContent = parts.length ? parts.join(' · ') : 'Rate topics & study gaps ⭐';
  updateNavBadges();
}

function updateNavBadges() {
  const studied = GAPS.filter(g=>isGapStudied(g.id)).length;
  const knowledgeCount = KNOWLEDGE.length + studied;
  const gapsCount = GAPS.length - studied;

  const kBadge = document.querySelector('[data-nav="knowledge"] .nav-badge');
  if (kBadge) kBadge.textContent = knowledgeCount;

  const gBadge = document.querySelector('[data-nav="gaps"] .nav-badge');
  if (gBadge) gBadge.textContent = gapsCount;

  const corrReviewed = CORRECTIONS.filter(c=>isCorrectionReviewed(c.id)).length;
  const cBadge = document.querySelector('[data-nav="corrections"] .nav-badge');
  if (cBadge) {
    cBadge.textContent = corrReviewed === CORRECTIONS.length ? '✓' : CORRECTIONS.length - corrReviewed + ' left';
  }
}

function showToast(msg) {
  const t = document.getElementById('score-toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2600);
}

// ---- ANIMATED COUNTERS ----
function animateCounter(el, target, duration = 800) {
  const start = performance.now();
  const from = 0;
  const step = (now) => {
    const progress = Math.min((now - start) / duration, 1);
    const ease = 1 - Math.pow(1 - progress, 3);
    el.textContent = Math.round(from + (target - from) * ease) + (el.dataset.suffix || '');
    if (progress < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

// ---- HERO STATS ----
function buildHeroStats() {
  const studied = GAPS.filter(g=>isGapStudied(g.id)).length;
  const score   = calcDynamicScore();

  const tEl = document.getElementById('stat-topics');
  const gEl = document.getElementById('stat-gaps');
  const sEl = document.getElementById('stat-score');
  const cEl = document.getElementById('stat-corrections');

  if (tEl) { tEl.dataset.suffix=''; animateCounter(tEl, KNOWLEDGE.length + studied); }
  if (gEl) { gEl.dataset.suffix=''; animateCounter(gEl, GAPS.length - studied); }
  if (sEl) { sEl.dataset.suffix='/100'; animateCounter(sEl, score, 1000); }
  if (cEl) { cEl.dataset.suffix=''; animateCounter(cEl, CORRECTIONS.length); }
}

function updateHeroScore() {
  const studied = GAPS.filter(g=>isGapStudied(g.id)).length;
  const prev    = parseInt(localStorage.getItem('mk_last_score') || '0');
  const score   = calcDynamicScore();
  const sEl = document.getElementById('stat-score');
  const tEl = document.getElementById('stat-topics');
  const gEl = document.getElementById('stat-gaps');
  if (sEl) { sEl.dataset.suffix='/100'; animateCounter(sEl, score, 400); }
  if (tEl) { tEl.dataset.suffix=''; animateCounter(tEl, KNOWLEDGE.length + studied, 300); }
  if (gEl) { gEl.dataset.suffix=''; animateCounter(gEl, GAPS.length - studied, 300); }
  if (document.getElementById('score-breakdown-panel')) updateReadinessPanel();
  updateSidebarScore();
  checkScoreMilestone(prev, score);
  localStorage.setItem('mk_last_score', score);
}

function checkScoreMilestone(prev, current) {
  const milestones = [
    { threshold: 80, key: 'mk_mile_80', msg: '🎉 Score hit 80! Solid junior-ready foundation!' },
    { threshold: 90, key: 'mk_mile_90', msg: '🚀 Score hit 90! Interview-ready for most roles!' },
    { threshold: 100, key: 'mk_mile_100', msg: '🏆 Perfect Score! You\'ve mastered the platform!' },
  ];
  milestones.forEach(m => {
    if (prev < m.threshold && current >= m.threshold && !localStorage.getItem(m.key)) {
      localStorage.setItem(m.key, '1');
      showMilestoneToast(m.msg);
    }
  });
}

function showMilestoneToast(msg) {
  const el = document.createElement('div');
  el.className = 'milestone-toast';
  el.textContent = msg;
  document.body.appendChild(el);
  requestAnimationFrame(() => el.classList.add('visible'));
  setTimeout(() => { el.classList.remove('visible'); setTimeout(() => el.remove(), 500); }, 4000);
}

// ---- DAILY RECOMMENDATION ----
function buildDailyRec() {
  const today = new Date().toDateString();
  let stored = null;
  try { stored = JSON.parse(localStorage.getItem('mk_daily_rec')); } catch(e) {}

  // Try gaps first (highest priority unstudied)
  const unstudiedGaps = GAPS.filter(g => !isGapStudied(g.id));
  const order = ['critical','high','medium','low'];
  let gapPick = null;
  for (const p of order) {
    const inP = unstudiedGaps.filter(g => g.priority === p);
    if (inP.length) { gapPick = inP[0]; break; }
  }

  // Fallback: unrated knowledge topic
  const unratedTopic = KNOWLEDGE.find(k => getConfidence(k.id) === 0);

  const container = document.getElementById('daily-rec');
  if (!container) return;

  if (gapPick) {
    container.innerHTML = `
      <div class="daily-rec" onclick="goToGap('${gapPick.id}')">
        <div class="daily-rec-icon">📅</div>
        <div class="daily-rec-content">
          <div class="daily-rec-label">Study Today · Gap</div>
          <div class="daily-rec-title">${gapPick.title}</div>
          <div class="daily-rec-reason">${gapPick.subtitle}</div>
        </div>
        <div class="daily-rec-action">Open →</div>
      </div>`;
  } else if (unratedTopic) {
    container.innerHTML = `
      <div class="daily-rec" onclick="navigate('knowledge')">
        <div class="daily-rec-icon">⭐</div>
        <div class="daily-rec-content">
          <div class="daily-rec-label">Rate Your Confidence</div>
          <div class="daily-rec-title">${unratedTopic.title}</div>
          <div class="daily-rec-reason">${unratedTopic.subtitle}</div>
        </div>
        <div class="daily-rec-action">Open →</div>
      </div>`;
  } else {
    // All gaps studied, all topics rated
    const lowestRated = [...KNOWLEDGE].sort((a,b) => getConfidence(a.id) - getConfidence(b.id))[0];
    container.innerHTML = `
      <div class="daily-rec" onclick="navigate('knowledge')">
        <div class="daily-rec-icon">💪</div>
        <div class="daily-rec-content">
          <div class="daily-rec-label">Review & Strengthen</div>
          <div class="daily-rec-title">${lowestRated.title}</div>
          <div class="daily-rec-reason">Your lowest confidence topic — keep it sharp before the interview.</div>
        </div>
        <div class="daily-rec-action">Open →</div>
      </div>`;
  }
}

function goToGap(id) {
  navigate('gaps');
  GAP_EXPANDED.add(id);
  setTimeout(() => {
    buildGaps(document.querySelector('.filter-btn.active')?.dataset.filter || 'all');
    const card = document.querySelector(`.gap-card[data-id="${id}"]`);
    if (card) card.scrollIntoView({ behavior:'smooth', block:'start' });
  }, 200);
}

// ---- KNOWLEDGE MAP ----
function buildKnowledge() {
  const container = document.getElementById('knowledge-content');
  const sections  = [...new Set(KNOWLEDGE.map(k=>k.section))];
  const studiedGaps = GAPS.filter(g=>isGapStudied(g.id));

  const studiedSection = studiedGaps.length > 0 ? `
    <div class="knowledge-section" id="studied-gaps-section">
      <div class="section-header">
        <span class="section-icon">✅</span>
        <h2>Studied from Gap Analysis</h2>
        <span class="badge badge-low" style="margin-left:8px">${studiedGaps.length} new</span>
      </div>
      <div class="card-grid">${studiedGaps.map(g=>buildStudiedGapCard(g)).join('')}</div>
    </div>` : `<div id="studied-gaps-section"></div>`;

  container.innerHTML = studiedSection + sections.map(section => {
    const items = KNOWLEDGE.filter(k=>k.section===section);
    return `
      <div class="knowledge-section">
        <div class="section-header">
          <span class="section-icon">${items[0].sectionIcon}</span>
          <h2>${section}</h2>
        </div>
        <div class="card-grid">${items.map(buildKnowledgeCard).join('')}</div>
      </div>`;
  }).join('');

  wireKnowledgeCards(container);

  // Expand-all / Collapse-all for Knowledge Map
  const ctrlRow = document.createElement('div');
  ctrlRow.style.cssText = 'display:flex;gap:8px;margin-bottom:16px;';
  ctrlRow.innerHTML = '<button class="filter-btn" id="km-expand-all">Expand All</button><button class="filter-btn" id="km-collapse-all">Collapse All</button>';
  container.prepend(ctrlRow);
  document.getElementById('km-expand-all').addEventListener('click', () =>
    container.querySelectorAll('.knowledge-card').forEach(c => c.classList.add('expanded')));
  document.getElementById('km-collapse-all').addEventListener('click', () =>
    container.querySelectorAll('.knowledge-card').forEach(c => c.classList.remove('expanded')));
}

function refreshStudiedSection() {
  const studiedGaps = GAPS.filter(g=>isGapStudied(g.id));
  const section = document.getElementById('studied-gaps-section');
  if (!section) return;
  if (!studiedGaps.length) { section.innerHTML=''; return; }
  section.innerHTML = `
    <div class="section-header">
      <span class="section-icon">✅</span>
      <h2>Studied from Gap Analysis</h2>
      <span class="badge badge-low" style="margin-left:8px">${studiedGaps.length} new</span>
    </div>
    <div class="card-grid">${studiedGaps.map(g=>buildStudiedGapCard(g)).join('')}</div>`;
  wireKnowledgeCards(section);
}

function buildStudiedGapCard(gap) {
  return `
    <div class="knowledge-card studied-gap-card" data-id="gap-${gap.id}">
      <div class="knowledge-card-header">
        <div style="flex:1">
          <div class="knowledge-card-title">${gap.title}</div>
          <div class="knowledge-card-subtitle">${gap.subtitle}</div>
        </div>
        <span class="studied-badge">✓ Studied</span>
        <div class="knowledge-card-toggle">▾</div>
      </div>
      <div class="knowledge-card-summary">"${gap.lesson.intro}"</div>
      <div class="knowledge-card-body">
        ${gap.lesson.types.map(t=>`
          <div class="knowledge-point">
            <div class="point-label">${t.name}</div>
            <div class="point-text">${t.desc}${t.example?`<div class="lesson-type-example" style="margin-top:8px">${t.example}</div>`:''}</div>
          </div>`).join('')}
      </div>
      <div class="knowledge-tags">
        <span class="tag">newly-learned</span>
        <span class="tag">${gap.priority}-priority</span>
      </div>
      <div class="confidence-row">
        <span class="confidence-label">Confidence</span>
        <div class="star-rating" role="radiogroup" aria-label="Confidence rating">${[1,2,3,4,5].map(n=>`<span class="star" data-star="${n}" role="radio" aria-label="${n} star" tabindex="0">★</span>`).join('')}</div>
        <span class="confidence-text"></span>
      </div>
    </div>`;
}

function buildKnowledgeCard(item) {
  const eli5 = ELI5[item.id] || '';
  const isEli5Open = ELI5_VISIBLE.has(item.id);
  return `
    <div class="knowledge-card" data-id="${item.id}">
      <div class="knowledge-card-header">
        <div style="flex:1">
          <div class="knowledge-card-title">${item.title}</div>
          <div class="knowledge-card-subtitle">${item.subtitle}</div>
        </div>
        ${eli5 ? `<button class="eli5-toggle-btn">${isEli5Open?'🧒 Hide ELI5':'🧒 ELI5'}</button>` : ''}
        <div class="knowledge-card-toggle">▾</div>
      </div>
      ${eli5 ? `<div class="eli5-panel${isEli5Open?' visible':''}">${eli5}</div>` : ''}
      <div class="knowledge-card-summary">"${item.summary}"</div>
      <div class="knowledge-card-body">
        ${item.points.map(p=>`
          <div class="knowledge-point">
            <div class="point-label">${p.label}</div>
            <div class="point-text">${p.text}</div>
          </div>`).join('')}
      </div>
      <div class="knowledge-tags">${item.tags.map(t=>`<span class="tag" role="button" tabindex="0" title="Search: ${t}">${t}</span>`).join('')}</div>
      ${item.relatedIds && item.relatedIds.length ? `<div class="related-chips"><span class="related-label">Related:</span>${item.relatedIds.map(rid=>`<span class="related-chip" data-id="${rid}">${(TOPIC_MAP[rid]||{}).title||rid} ↗</span>`).join('')}</div>` : ''}
      <div class="confidence-row">
        <span class="confidence-label">Confidence</span>
        <div class="star-rating" role="radiogroup" aria-label="Confidence rating">${[1,2,3,4,5].map(n=>`<span class="star" data-star="${n}" role="radio" aria-label="${n} star" tabindex="0">★</span>`).join('')}</div>
        <span class="confidence-text"></span>
      </div>
    </div>`;
}

function wireKnowledgeCards(container) {
  container.querySelectorAll('.knowledge-card').forEach(card => {
    const rawId = card.dataset.id || '';
    const isGap = rawId.startsWith('gap-');
    const id    = isGap ? rawId.replace('gap-','') : rawId;

    card.querySelector('.knowledge-card-header').addEventListener('click', e => {
      if (e.target.closest('.eli5-toggle-btn')) return;
      card.classList.toggle('expanded');
    });

    // P1a: tag → search
    card.querySelectorAll('.tag').forEach(tag => {
      const activate = () => { openSearch(); const f = document.getElementById('search-field'); if (f) { f.value = tag.textContent; doSearch(tag.textContent); } };
      tag.addEventListener('click', e => { e.stopPropagation(); activate(); });
      tag.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activate(); } });
    });

    // P1b: relatedIds → navigate to card
    card.querySelectorAll('.related-chip').forEach(chip => {
      chip.addEventListener('click', e => {
        e.stopPropagation();
        navigate('knowledge');
        setTimeout(() => {
          const target = document.querySelector(`.knowledge-card[data-id="${chip.dataset.id}"]`);
          if (target) { target.classList.add('expanded'); target.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
        }, 200);
      });
    });

    // ELI5 — restore state + wire toggle
    const eli5Btn   = card.querySelector('.eli5-toggle-btn');
    const eli5Panel = card.querySelector('.eli5-panel');
    if (eli5Btn && eli5Panel) {
      eli5Btn.addEventListener('click', e => {
        e.stopPropagation();
        const open = eli5Panel.classList.toggle('visible');
        eli5Btn.textContent = open ? '🧒 Hide ELI5' : '🧒 ELI5';
        if (open) ELI5_VISIBLE.add(id); else ELI5_VISIBLE.delete(id);
      });
    }

    // Stars
    const stars    = card.querySelectorAll('.star');
    const confText = card.querySelector('.confidence-text');
    const confKey  = isGap ? 'gap-' + id : id;
    const saved    = getConfidence(confKey);
    if (saved > 0) highlightStars(stars, saved, confText);

    stars.forEach(star => {
      const activateStar = () => {
        const v = parseInt(star.dataset.star);
        setConfidence(confKey, v);
        highlightStars(stars, v, confText);
        star.classList.add('star-pop');
        setTimeout(() => star.classList.remove('star-pop'), 300);
        updateHeroScore();
        const title = isGap ? GAPS.find(g=>g.id===id)?.title : TOPIC_MAP[id]?.title;
        showToast(`"${title}" rated ${v}/5 ⭐`);
      };
      star.addEventListener('click', activateStar);
      star.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activateStar(); } });
      star.addEventListener('mouseenter', () => {
        const v = parseInt(star.dataset.star);
        stars.forEach(s => s.classList.toggle('active', parseInt(s.dataset.star)<=v));
      });
      star.addEventListener('mouseleave', () => {
        const sv = getConfidence(confKey);
        stars.forEach(s => s.classList.toggle('active', parseInt(s.dataset.star)<=sv));
      });
    });
  });
}

function highlightStars(stars, val, confText) {
  stars.forEach(s => s.classList.toggle('active', parseInt(s.dataset.star) <= val));
  if (confText) confText.textContent = CONFIDENCE_LABELS[val] || '';
}

// ---- CORRECTIONS ----
function buildCorrections() {
  const container = document.getElementById('corrections-content');
  container.innerHTML = CORRECTIONS.map(c => {
    const done = isCorrectionReviewed(c.id);
    return `
      <div class="correction-card ${done?'reviewed':''}" data-id="${c.id}">
        <div class="correction-header">
          <span class="correction-icon">⚠️</span>
          <div class="correction-title">${c.title}</div>
          ${done?'<span class="reviewed-badge">✓ Reviewed</span>':''}
        </div>
        <div class="correction-field">
          <div class="correction-field-label">What you said</div>
          <div class="correction-field-value yours">${c.yourStatement}</div>
        </div>
        <div class="correction-field">
          <div class="correction-field-label">Correct understanding</div>
          <div class="correction-field-value fixed">${c.correction}</div>
        </div>
        <div class="correction-rule">${c.rule}</div>
        <div class="mark-action-row">
          <button class="mark-reviewed-btn ${done?'done':''}" data-id="${c.id}">
            ${done ? '✓ Marked as Understood' : '📌 Mark as Understood (+1pt)'}
          </button>
          ${done?`<button class="unmark-btn" data-id="${c.id}">Undo</button>`:''}
        </div>
      </div>`;
  }).join('');
  container.querySelectorAll('.mark-reviewed-btn, .unmark-btn').forEach(btn =>
    btn.addEventListener('click', () => markCorrectionReviewed(btn.dataset.id)));
}

function markCorrectionReviewed(id) {
  const was = isCorrectionReviewed(id);
  setCorrectionReviewed(id, !was);
  buildCorrections();
  updateHeroScore();
  const c = CORRECTIONS.find(x=>x.id===id);
  showToast(was ? `"${c.title}" unmarked` : `Correction understood! +1pt 🎯`);
}

// ---- GAP ANALYSIS ----
function buildGaps(filter = 'all') {
  // capture currently expanded before rebuild
  document.querySelectorAll('.gap-card.expanded').forEach(c => GAP_EXPANDED.add(c.dataset.id));

  const container = document.getElementById('gaps-content');
  const filtered  = filter==='all' ? GAPS : GAPS.filter(g=>g.priority===filter);

  container.innerHTML = filtered.map(gap => {
    const done  = isGapStudied(gap.id);
    const pts   = GAP_SCORE_WEIGHTS[gap.id] || 0;
    const isExp = GAP_EXPANDED.has(gap.id);
    const connectItems = gap.connectsTo.map(cid => {
      const f = KNOWLEDGE.find(k=>k.id===cid)||GAPS.find(g=>g.id===cid);
      return { id: cid, title: f ? f.title : cid, isKnowledge: !!KNOWLEDGE.find(k=>k.id===cid) };
    });
    return `
      <div class="gap-card ${done?'studied':''} ${isExp?'expanded':''}" data-id="${gap.id}" data-priority="${gap.priority}">
        <div class="gap-card-header">
          <div class="gap-card-left">
            <div class="gap-card-title">
              ${gap.title}
              ${done?'<span class="studied-badge">✓ In Your Knowledge</span>':''}
            </div>
            <div class="gap-card-subtitle">${gap.subtitle}</div>
            <div class="gap-connects">
              <span class="gap-connects-label">Connects to:</span>
              ${connectItems.map(c=>`<span class="connects-tag" data-id="${c.id}" data-is-knowledge="${c.isKnowledge}" role="button" tabindex="0" title="Go to ${c.title}">${c.title} ↗</span>`).join('')}
            </div>
          </div>
          <div class="gap-card-right">
            <span class="badge badge-${gap.priority}">${gap.priority}</span>
            ${pts>0?`<span class="pts-badge">+${pts}pts</span>`:''}
            <div class="gap-toggle">▾</div>
          </div>
        </div>
        <div class="gap-card-body">
          <div class="gap-why"><strong>Why this matters:</strong> ${gap.whyItMatters}</div>
          <div class="gap-connection-note">${gap.connectionExplain}</div>
          <div class="lesson-intro">${gap.lesson.intro}</div>
          <div class="lesson-types">
            ${gap.lesson.types.map(t=>`
              <div class="lesson-type">
                <div class="lesson-type-name">${t.name}</div>
                <div class="lesson-type-desc">${t.desc}</div>
                ${t.example?`<div class="lesson-type-example">${t.example}</div>`:''}
              </div>`).join('')}
          </div>
          <div class="mark-action-row">
            <button class="mark-studied-btn ${done?'done':''}" data-id="${gap.id}">
              ${done ? '✓ Moved to Your Knowledge' : `✅ Mark as Studied${pts>0?` (+${pts}pts)`:''}`}
            </button>
            ${done?`<button class="unmark-btn" data-id="${gap.id}">Undo</button>`:''}
          </div>
        </div>
      </div>`;
  }).join('') || `<div class="empty-state"><div class="empty-icon">✅</div><p>No gaps in this category</p></div>`;

  container.querySelectorAll('.gap-card-header').forEach(h =>
    h.addEventListener('click', () => {
      const card = h.closest('.gap-card');
      const id   = card.dataset.id;
      card.classList.toggle('expanded');
      if (card.classList.contains('expanded')) GAP_EXPANDED.add(id);
      else GAP_EXPANDED.delete(id);
    }));

  container.querySelectorAll('.mark-studied-btn').forEach(btn =>
    btn.addEventListener('click', e => { e.stopPropagation(); markGapStudied(btn.dataset.id); }));
  container.querySelectorAll('.unmark-btn').forEach(btn =>
    btn.addEventListener('click', e => { e.stopPropagation(); markGapStudied(btn.dataset.id); }));

  // P1c: connects-tag → navigate to knowledge card
  container.querySelectorAll('.connects-tag').forEach(tag => {
    const go = () => {
      if (tag.dataset.isKnowledge === 'true') {
        navigate('knowledge');
        setTimeout(() => {
          const target = document.querySelector(`.knowledge-card[data-id="${tag.dataset.id}"]`);
          if (target) { target.classList.add('expanded'); target.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
        }, 200);
      } else {
        navigate('gaps');
        setTimeout(() => {
          const target = document.querySelector(`.gap-card[data-id="${tag.dataset.id}"]`);
          if (target) { target.classList.add('expanded'); target.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
        }, 200);
      }
    };
    tag.addEventListener('click', e => { e.stopPropagation(); go(); });
    tag.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
  });
}

function markGapStudied(id) {
  const was = isGapStudied(id);
  setGapStudied(id, !was);
  const activeFilter = document.querySelector('.filter-btn.active')?.dataset.filter || 'all';
  buildGaps(activeFilter);
  refreshStudiedSection();
  updateHeroScore();
  buildDailyRec();
  const gap = GAPS.find(g=>g.id===id);
  const pts = GAP_SCORE_WEIGHTS[id]||0;
  showToast(was
    ? `"${gap.title}" removed from your knowledge`
    : `"${gap.title}" added to your knowledge!${pts>0?` +${pts}pts 🚀`:''}`);
}

// ---- CONNECTIONS ----
function buildConnections() {
  const el = document.getElementById('connections-content');
  el.innerHTML = CONNECTIONS.map(conn => `
    <div class="connection-card">
      <div class="connection-nodes">
        <span class="connection-node from conn-link" data-id="${conn.from}" title="Go to ${conn.fromLabel}">${conn.fromLabel} ↗</span>
        <span class="connection-arrow">⟶</span>
        <span class="connection-node to conn-link" data-id="${conn.to}" title="Go to ${conn.toLabel}">${conn.toLabel} ↗</span>
      </div>
      <div class="connection-text">${conn.relation}</div>
    </div>`).join('');

  el.querySelectorAll('.conn-link').forEach(node => {
    node.addEventListener('click', () => {
      navigate('knowledge');
      setTimeout(() => {
        const card = document.querySelector(`.knowledge-card[data-id="${node.dataset.id}"]`);
        if (card) {
          card.classList.add('expanded');
          card.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }, 200);
    });
  });
}

// ---- INTERVIEW READINESS ----

// Map topic/gap IDs → category index (matches INTERVIEW_READINESS.categories order)
const CATEGORY_TOPIC_MAP = [
  // 0: Data Warehouse Concepts
  ['oltp-olap','dw-lake-lakehouse','fact-dimension','normalization','acid-transactions'],
  // 1: Dimensional Modeling
  ['star-snowflake','fact-dimension','scd-types','merge-upsert'],
  // 2: Pipeline Design
  ['pipeline-phases','incremental-loading','etl-elt','idempotency','medallion-arch','delta-iceberg','parquet-format'],
  // 3: Apache Airflow
  ['airflow','dag-linearity','airflow-scheduling'],
  // 4: Apache Spark
  ['spark-architecture','spark-rdd-partitions','spark-shuffle','spark-cache-persist','spark-lazy-eval','wide-narrow-transforms','data-skew'],
  // 5: SQL (Intermediate+)
  ['sql-joins','advanced-sql','storage-partitioning'],
  // 6: Cloud Platforms
  ['cloud-platforms'],
  // 7: dbt
  ['dbt-tool'],
  // 8: Streaming / Real-time
  ['streaming-kafka'],
  // 9: Data Quality & Testing
  ['data-quality','docker-de'],
];

function computeLiveCategories() {
  return INTERVIEW_READINESS.categories.map((cat, i) => {
    const ids = CATEGORY_TOPIC_MAP[i] || [];
    const ratings = ids.map(id => getConfidence(id)).filter(r => r > 0);
    let score = cat.score;
    if (ratings.length > 0) {
      const avgStar = ratings.reduce((a,b)=>a+b,0) / ratings.length; // 1-5
      const ratedPct = avgStar / 5 * 100;
      const coverage = ratings.length / ids.length; // how many of the topics rated
      // blend: base 40% + live ratings 60% (weighted by coverage)
      score = Math.round(cat.score * (1 - coverage * 0.6) + ratedPct * (coverage * 0.6));
      score = Math.max(0, Math.min(100, score));
    }
    let status = cat.status;
    if (score >= 80) status = 'strong';
    else if (score >= 65) status = 'good';
    else if (score >= 45) status = 'decent';
    else if (score >= 25) status = 'gap';
    else status = score < 20 ? 'critical-gap' : 'needs-work';
    return { ...cat, score, status };
  });
}

function computeLiveWeakPoints() {
  // Build a scored list of all topics — lower confidence = higher priority
  const allTopics = KNOWLEDGE.map(k => {
    const conf = getConfidence(k.id);
    return { id: k.id, title: k.title, conf, isGap: false };
  });
  GAPS.forEach(g => {
    const conf = getConfidence(g.id);
    allTopics.push({ id: g.id, title: g.title, conf, isGap: true });
  });
  // Sort: unrated (0) first, then by ascending confidence
  const weakest = allTopics
    .filter(t => t.conf < 3) // below "comfortable"
    .sort((a,b) => a.conf - b.conf || a.title.localeCompare(b.title))
    .slice(0, 5);
  if (weakest.length === 0) return INTERVIEW_READINESS.weakPoints; // fallback
  return weakest.map((t, i) => {
    const orig = INTERVIEW_READINESS.weakPoints.find(w => w.topic.toLowerCase().includes(t.title.toLowerCase().split(' ')[0]));
    const detail = orig ? orig.detail : `Rated ${t.conf}/5 — needs more practice before interviews.`;
    const impact = t.conf === 0 ? 'High' : t.conf < 2 ? 'Medium-High' : 'Medium';
    return { rank: i+1, topic: t.title, impact, detail };
  });
}

function buildReadiness() {
  const r    = INTERVIEW_READINESS;
  const circ = 2 * Math.PI * 65;
  const cats = computeLiveCategories();
  const wps  = computeLiveWeakPoints();
  document.getElementById('readiness-content').innerHTML = `
    <div id="score-breakdown-panel"></div>
    <div class="readiness-overview">
      <div class="score-gauge">
        <svg viewBox="0 0 160 160">
          <defs>
            <linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" style="stop-color:#8b5cf6"/>
              <stop offset="100%" style="stop-color:#22d3ee"/>
            </linearGradient>
          </defs>
          <circle class="score-gauge-track" cx="80" cy="80" r="65"/>
          <circle class="score-gauge-fill" cx="80" cy="80" r="65"
            style="stroke-dasharray:${circ};stroke-dashoffset:${circ};"
            data-circ="${circ}"/>
        </svg>
        <div class="score-gauge-center">
          <span class="score-gauge-number" id="gauge-number">—</span>
          <span class="score-gauge-label">/ 100</span>
        </div>
      </div>
      <div class="readiness-info">
        <h2>${r.level}</h2>
        <p>${r.summary}</p>
        <div style="margin-top:12px;font-size:13px;color:var(--text-muted)">
          Max: <strong style="color:var(--purple-light)">100/100</strong>
          — rate all topics 5★ + study all gaps + review all corrections
        </div>
      </div>
    </div>
    <div class="readiness-categories">
      <h3>Knowledge Breakdown <span style="font-size:12px;font-weight:400;color:var(--text-muted)">(updates with your star ratings)</span></h3>
      ${cats.map(cat=>`
        <div class="category-row" title="${cat.note}">
          <div class="category-name">${cat.name}</div>
          <div class="category-bar">
            <div class="category-bar-fill bar-${cat.status}" style="width:${cat.score}%"></div>
          </div>
          <div class="category-score score-${cat.status}">${cat.score}%</div>
          <div class="category-note">${cat.note}</div>
        </div>`).join('')}
    </div>
    <div class="divider"></div>
    <div class="weak-points">
      <h3>Priority Improvements <span style="font-size:12px;font-weight:400;color:var(--text-muted)">(based on your ratings)</span></h3>
      ${wps.map(wp=>`
        <div class="weak-point-card">
          <div class="weak-point-rank">${wp.rank}</div>
          <div>
            <div class="weak-point-topic">${wp.topic}</div>
            <div class="weak-point-detail">${wp.detail}</div>
          </div>
          <span class="impact-badge impact-${wp.impact.replace(/ /g,'-')}">${wp.impact}</span>
        </div>`).join('')}
    </div>`;
  updateReadinessPanel();
}

function updateReadinessPanel() {
  const panel = document.getElementById('score-breakdown-panel');
  if (!panel) return;
  const score = calcDynamicScore();
  const bd    = getScoreBreakdown();
  const studiedPts  = Math.round(GAPS.reduce((s,g)=>s+(isGapStudied(g.id)?GAP_SCORE_WEIGHTS[g.id]||0:0),0)*10)/10;
  const reviewedPts = CORRECTIONS.filter(c=>isCorrectionReviewed(c.id)).length * CORRECTION_SCORE_WEIGHT;

  panel.innerHTML = `
    <div class="score-breakdown">
      <div class="sbd-title">Live Score Breakdown</div>
      <div class="sbd-rows">
        <div class="sbd-row">
          <span class="sbd-label">Base (assessed knowledge)</span>
          <span class="sbd-val">${BASE_SCORE}</span>
        </div>
        <div class="sbd-row">
          <span class="sbd-label">Topic confidence (${bd.rated}/15 rated) · max ±${MAX_TOPIC_DELTA}</span>
          <span class="sbd-val ${bd.topicDelta>=0?'pos':'neg'}">${bd.topicDelta>=0?'+':''}${bd.topicDelta}</span>
        </div>
        <div class="sbd-row">
          <span class="sbd-label">Gaps studied (${bd.studied}/${GAPS.length}) · max +${MAX_GAP_PTS}</span>
          <span class="sbd-val ${studiedPts>0?'pos':''}">${studiedPts>0?'+':''}${studiedPts}</span>
        </div>
        <div class="sbd-row">
          <span class="sbd-label">Corrections reviewed (${bd.reviewed}/${CORRECTIONS.length}) · max +${MAX_CORR_PTS}</span>
          <span class="sbd-val ${reviewedPts>0?'pos':''}">${reviewedPts>0?'+':''}${reviewedPts}</span>
        </div>
        <div class="sbd-divider"></div>
        <div class="sbd-row total">
          <span class="sbd-label">Current Score</span>
          <span class="sbd-val total-val">${score} / 100</span>
        </div>
      </div>
    </div>`;

  const gaugeNum = document.getElementById('gauge-number');
  if (gaugeNum) gaugeNum.textContent = score;

  // Refresh live category bars
  const cats = computeLiveCategories();
  const catRows = document.querySelectorAll('.category-row');
  if (catRows.length === cats.length) {
    catRows.forEach((row, i) => {
      const cat = cats[i];
      const fill = row.querySelector('.category-bar-fill');
      const scoreEl = row.querySelector('.category-score');
      if (fill) { fill.className = `category-bar-fill bar-${cat.status}`; fill.style.width = cat.score + '%'; }
      if (scoreEl) { scoreEl.className = `category-score score-${cat.status}`; scoreEl.textContent = cat.score + '%'; }
    });
  }

  // Refresh weak points
  const wps = computeLiveWeakPoints();
  const wpContainer = document.querySelector('.weak-points');
  if (wpContainer) {
    const h3 = wpContainer.querySelector('h3');
    wpContainer.innerHTML = '';
    if (h3) wpContainer.appendChild(h3);
    wps.forEach(wp => {
      const div = document.createElement('div');
      div.className = 'weak-point-card';
      div.innerHTML = `<div class="weak-point-rank">${wp.rank}</div><div><div class="weak-point-topic">${wp.topic}</div><div class="weak-point-detail">${wp.detail}</div></div><span class="impact-badge impact-${wp.impact.replace(/ /g,'-')}">${wp.impact}</span>`;
      wpContainer.appendChild(div);
    });
  }
}

function animateReadiness() {
  updateReadinessPanel();
  const fill = document.querySelector('.score-gauge-fill');
  if (!fill) return;
  const score = calcDynamicScore();
  const circ  = parseFloat(fill.dataset.circ);
  // reset first so animation replays on every visit
  fill.style.transition = 'none';
  fill.style.strokeDashoffset = circ;
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      fill.style.transition = '';
      fill.style.strokeDashoffset = circ - (score / MAX_SCORE) * circ;
    });
  });
  // reset bars then animate
  document.querySelectorAll('.category-bar-fill').forEach(b => {
    b.style.transition = 'none';
    b.style.transform  = 'scaleX(0)';
  });
  setTimeout(() => {
    document.querySelectorAll('.category-bar-fill').forEach(b => {
      b.style.transition = '';
      b.style.transform  = 'scaleX(1)';
    });
  }, 80);
  const gaugeNum = document.getElementById('gauge-number');
  if (gaugeNum) gaugeNum.textContent = score;
}

// ---- FLASHCARDS ----
let quizState = { topicId:null, cards:[], index:0, flipped:false, results:[] };

function buildFlashcards() {
  const container = document.getElementById('flashcards-content');
  const options = FLASHCARDS.map(f => {
    const t = TOPIC_MAP[f.topicId];
    return `<option value="${f.topicId}">${t?t.title:f.topicId}</option>`;
  }).join('');

  container.innerHTML = `
    <div class="quiz-controls">
      <select class="topic-selector" id="quiz-topic-select">${options}</select>
      <div class="quiz-progress" id="quiz-progress"></div>
      <div class="quiz-score-badge" id="quiz-score-badge" style="display:none"></div>
    </div>
    <div class="flashcard-dots" id="flashcard-dots"></div>
    <div class="flashcard-container" id="flashcard-container">
      <div class="flashcard" id="flashcard">
        <div class="flashcard-face flashcard-front">
          <div class="flashcard-hint">Question</div>
          <div class="flashcard-text" id="card-front-text"></div>
          <div class="flashcard-tap">Tap or press <kbd>Space</kbd> to reveal</div>
        </div>
        <div class="flashcard-face flashcard-back">
          <div class="flashcard-hint">Answer</div>
          <div class="flashcard-text" id="card-back-text"></div>
        </div>
      </div>
    </div>
    <div class="flashcard-actions hidden" id="flashcard-actions">
      <button class="fc-btn fc-btn-again" id="btn-again">🔄 Review Again <span class="fc-key">←</span></button>
      <button class="fc-btn fc-btn-got"   id="btn-got">✓ Got It <span class="fc-key">→</span></button>
    </div>
    <div class="quiz-complete" id="quiz-complete">
      <div class="quiz-complete-icon" id="quiz-complete-icon">🎉</div>
      <h2 id="quiz-complete-title">Session Complete!</h2>
      <p id="quiz-complete-msg"></p>
      <button class="fc-btn fc-btn-next" onclick="restartQuiz()">🔁 Restart</button>
    </div>`;

  document.getElementById('quiz-topic-select').addEventListener('change', e=>startQuiz(e.target.value));
  document.getElementById('flashcard').addEventListener('click', flipCard);
  document.getElementById('btn-again').addEventListener('click', ()=>recordAnswer('again'));
  document.getElementById('btn-got').addEventListener('click', ()=>recordAnswer('got'));
  startQuiz(FLASHCARDS[0].topicId);
}

// keyboard handler — only active when flashcards section is visible
document.addEventListener('keydown', e => {
  const fc = document.getElementById('flashcards');
  if (!fc || !fc.classList.contains('active')) return;
  if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
  if (e.key === ' ' || e.key === 'Enter') { e.preventDefault(); flipCard(); }
  if (e.key === 'ArrowRight') { e.preventDefault(); if (!document.getElementById('flashcard-actions').classList.contains('hidden')) recordAnswer('got'); }
  if (e.key === 'ArrowLeft')  { e.preventDefault(); if (!document.getElementById('flashcard-actions').classList.contains('hidden')) recordAnswer('again'); }
});

function startQuiz(topicId) {
  const data = FLASHCARDS.find(f=>f.topicId===topicId);
  if (!data) return;
  quizState = { topicId, cards:[...data.cards], index:0, flipped:false, results:data.cards.map(()=>null) };
  document.getElementById('quiz-complete').classList.remove('visible');
  document.getElementById('flashcard-container').style.display='';
  renderCard();
}

function renderCard() {
  const { cards, index, results } = quizState;
  const card = cards[index];
  document.getElementById('card-front-text').textContent = card.front;
  document.getElementById('card-back-text').textContent  = card.back;
  document.getElementById('flashcard').classList.remove('flipped');
  document.getElementById('flashcard-actions').classList.add('hidden');
  quizState.flipped = false;
  document.getElementById('quiz-progress').textContent = `${index+1} / ${cards.length}`;
  const got  = results.filter(r=>r==='got').length;
  const done = results.filter(r=>r!==null).length;
  const badge = document.getElementById('quiz-score-badge');
  if (done > 0) { badge.style.display=''; badge.textContent=`${got}/${done} Got It`; }
  const dots = document.getElementById('flashcard-dots');
  dots.innerHTML = cards.map((_,i) => {
    const cls = results[i]==='got'?'got':results[i]==='again'?'again':i===index?'current':'';
    return `<div class="fc-dot ${cls}"></div>`;
  }).join('');
}

function flipCard() {
  if (quizState.flipped) return;
  quizState.flipped = true;
  document.getElementById('flashcard').classList.add('flipped');
  setTimeout(()=>document.getElementById('flashcard-actions').classList.remove('hidden'), 260);
}

function recordAnswer(result) {
  quizState.results[quizState.index] = result;
  const next = findNextCard();
  if (next === -1) showQuizComplete();
  else { quizState.index = next; renderCard(); }
}

function findNextCard() {
  const { cards, results, index } = quizState;
  for (let i=index+1;i<cards.length;i++) if (results[i]===null) return i;
  for (let i=0;i<index;i++) if (results[i]===null) return i;
  return -1;
}

function showQuizComplete() {
  const { results } = quizState;
  const got = results.filter(r=>r==='got').length;
  const pct = Math.round((got/results.length)*100);
  document.getElementById('flashcard-container').style.display='none';
  document.getElementById('flashcard-actions').classList.add('hidden');
  const complete = document.getElementById('quiz-complete');
  complete.classList.add('visible');
  document.getElementById('quiz-complete-icon').textContent  = pct>=80?'🎉':pct>=50?'💪':'📖';
  document.getElementById('quiz-complete-title').textContent = pct>=80?'Excellent!':pct>=50?'Good Progress!':'Keep Practicing!';
  document.getElementById('quiz-complete-msg').textContent   = `${got}/${results.length} correct (${pct}%). ${pct<100?'Review the ones you missed.':'Perfect score!'}`;
  document.getElementById('quiz-progress').textContent = `${got}/${results.length} Got It`;
}

function restartQuiz() { startQuiz(quizState.topicId); }

// ---- INTERVIEW QUESTIONS ----
function buildInterviewQuestions(filterTopic='all', filterDiff='all', filterPracticed='all') {
  const container = document.getElementById('iq-content');

  // Stable global flat list — index = localStorage key
  const globalFlat = INTERVIEW_QUESTIONS.flatMap(iq =>
    iq.questions.map((q, i) => ({ ...q, topicId: iq.topicId, globalIdx: 0 }))
  );
  let gi = 0;
  INTERVIEW_QUESTIONS.forEach(iq =>
    iq.questions.forEach((q, i) => { globalFlat[gi].globalIdx = gi; gi++; })
  );

  const topicOpts = [`<option value="all">All Topics</option>`,
    ...INTERVIEW_QUESTIONS.map(iq => {
      const t = TOPIC_MAP[iq.topicId];
      return `<option value="${iq.topicId}">${t ? t.title : iq.topicId}</option>`;
    })].join('');

  let visible = [...globalFlat];
  if (filterTopic !== 'all')     visible = visible.filter(q => q.topicId === filterTopic);
  if (filterDiff !== 'all')      visible = visible.filter(q => q.difficulty === filterDiff);
  if (filterPracticed === 'not') visible = visible.filter(q => !isQPracticed(q.topicId, q.globalIdx));
  if (filterPracticed === 'done') visible = visible.filter(q => isQPracticed(q.topicId, q.globalIdx));

  const practicedCount = globalFlat.filter(q => isQPracticed(q.topicId, q.globalIdx)).length;

  container.innerHTML = `
    <div class="iq-filter-row">
      <select class="iq-topic-select" id="iq-select">${topicOpts}</select>
      <select class="iq-topic-select" id="iq-diff-select" style="min-width:130px">
        <option value="all">All Levels</option>
        <option value="easy">Easy</option>
        <option value="medium">Medium</option>
      </select>
      <select class="iq-topic-select" id="iq-practiced-select" style="min-width:150px">
        <option value="all">All Questions</option>
        <option value="not">Not Practiced</option>
        <option value="done">✓ Practiced</option>
      </select>
      <span class="iq-count">${visible.length} shown · <span class="iq-practiced-count">${practicedCount}/${globalFlat.length} practiced</span></span>
    </div>
    ${visible.map(q => {
      const done = isQPracticed(q.topicId, q.globalIdx);
      return `
      <div class="iq-card ${done ? 'iq-practiced' : ''}" data-topic="${q.topicId}" data-gidx="${q.globalIdx}">
        <div class="iq-card-header">
          <span class="iq-q-icon">${done ? '✅' : '❓'}</span>
          <div class="iq-question-text">${q.q}</div>
          <div class="iq-meta">
            ${done ? '<span class="iq-done-badge">Practiced</span>' : ''}
            <span class="badge diff-${q.difficulty}">${q.difficulty}</span>
            <span class="iq-toggle">▾</span>
          </div>
        </div>
        <div class="iq-card-body">
          <div class="iq-answer">${renderMarkdown(q.a)}</div>
          <div class="iq-tip">${q.tip}</div>
          <div class="mark-action-row" style="margin-top:14px">
            <button class="mark-practiced-btn ${done ? 'done' : ''}" data-topic="${q.topicId}" data-gidx="${q.globalIdx}">
              ${done ? '✓ Marked as Practiced' : '🎯 Mark as Practiced'}
            </button>
            ${done ? `<button class="unmark-btn iq-unmark" data-topic="${q.topicId}" data-gidx="${q.globalIdx}">Undo</button>` : ''}
          </div>
        </div>
      </div>`;
    }).join('') || '<div class="empty-state"><div class="empty-icon">✅</div><p>No questions match this filter</p></div>'}`;

  container.querySelectorAll('.iq-card').forEach(card =>
    card.querySelector('.iq-card-header').addEventListener('click', () => card.classList.toggle('expanded')));

  container.querySelectorAll('.mark-practiced-btn, .iq-unmark').forEach(btn =>
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const { topic, gidx } = btn.dataset;
      const was = isQPracticed(topic, gidx);
      setQPracticed(topic, gidx, !was);
      buildInterviewQuestions(
        document.getElementById('iq-select')?.value || 'all',
        document.getElementById('iq-diff-select')?.value || 'all',
        document.getElementById('iq-practiced-select')?.value || 'all'
      );
      showToast(was ? 'Question unmarked' : '🎯 Question marked as practiced!');
    }));

  const topicSel     = document.getElementById('iq-select');
  const diffSel      = document.getElementById('iq-diff-select');
  const practicedSel = document.getElementById('iq-practiced-select');
  topicSel.value     = filterTopic;
  diffSel.value      = filterDiff;
  practicedSel.value = filterPracticed;

  const rebuild = () => buildInterviewQuestions(topicSel.value, diffSel.value, practicedSel.value);
  topicSel.addEventListener('change', rebuild);
  diffSel.addEventListener('change', rebuild);
  practicedSel.addEventListener('change', rebuild);
}

// ---- CODE SNIPPETS ----
function buildSnippets() {
  const container = document.getElementById('snippets-content');
  container.innerHTML = CODE_SNIPPETS.map(cs=>{
    const topic = TOPIC_MAP[cs.topicId];
    return `
      <div class="snippets-topic-group">
        <div class="snippets-topic-header">
          <span>${topic?topic.sectionIcon:'💻'}</span>
          <div class="snippets-topic-name">${topic?topic.title:cs.topicId}</div>
        </div>
        ${cs.snippets.map((s,i)=>`
          <div class="snippet-card" data-id="${cs.topicId}-${i}">
            <div class="snippet-header">
              <span class="snippet-lang lang-${s.language}">${s.language}</span>
              <span class="snippet-title">${s.title}</span>
              <button class="copy-btn" data-code="${encodeURIComponent(s.code)}">Copy</button>
              <span class="snippet-toggle">▾</span>
            </div>
            <div class="snippet-body">
              <pre class="snippet-code"><code class="language-${s.language === 'sql' ? 'sql' : s.language === 'bash' ? 'bash' : s.language === 'yaml' ? 'yaml' : 'python'}">${escapeHtml(s.code)}</code></pre>
              <div class="snippet-explanation">${s.explanation}</div>
            </div>
          </div>`).join('')}
      </div>`;
  }).join('');

  // Expand all button (inject above first group)
  const expandAllBtn = document.createElement('div');
  expandAllBtn.style.cssText = 'margin-bottom:16px;display:flex;gap:8px;';
  expandAllBtn.innerHTML = `
    <button class="filter-btn" id="expand-all-btn">Expand All</button>
    <button class="filter-btn" id="collapse-all-btn">Collapse All</button>`;
  container.prepend(expandAllBtn);

  document.getElementById('expand-all-btn').addEventListener('click',()=>{
    container.querySelectorAll('.snippet-card').forEach(c=>c.classList.add('expanded'));
    prismHighlight(container);
  });
  document.getElementById('collapse-all-btn').addEventListener('click',()=>
    container.querySelectorAll('.snippet-card').forEach(c=>c.classList.remove('expanded')));

  container.querySelectorAll('.snippet-card').forEach(card=>{
    card.querySelector('.snippet-header').addEventListener('click',e=>{
      if (e.target.classList.contains('copy-btn')) return;
      card.classList.toggle('expanded');
      if (card.classList.contains('expanded')) prismHighlight(card);
    });
  });

  container.querySelectorAll('.copy-btn').forEach(btn=>{
    btn.addEventListener('click', e=>{
      e.stopPropagation();
      const code = decodeURIComponent(btn.dataset.code);
      const doFallback = () => {
        const ta = document.createElement('textarea');
        ta.value = code; ta.style.position='fixed'; ta.style.opacity='0';
        document.body.appendChild(ta); ta.focus(); ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      };
      if (navigator.clipboard) {
        navigator.clipboard.writeText(code).then(()=>{}).catch(doFallback);
      } else { doFallback(); }
      btn.textContent='Copied!'; btn.classList.add('copied');
      setTimeout(()=>{btn.textContent='Copy';btn.classList.remove('copied');},2000);
    });
  });
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function prismHighlight(el) {
  if (typeof Prism !== 'undefined') Prism.highlightAllUnder(el);
}

// ---- SEARCH ----
let searchIndex = [];

function buildSearchIndex() {
  searchIndex = [
    ...KNOWLEDGE.map(k=>({ type:'topic', id:k.id, nav:'knowledge', title:k.title, subtitle:k.section, text:[k.subtitle,k.summary,...k.points.map(p=>p.text)].join(' ') })),
    ...GAPS.map(g=>({ type:'gap', id:g.id, nav:'gaps', title:g.title, subtitle:g.subtitle, text:[g.whyItMatters,g.lesson.intro].join(' ') })),
    ...INTERVIEW_QUESTIONS.flatMap(iq=>iq.questions.map(q=>({ type:'question', id:iq.topicId, nav:'interview-questions', title:q.q, subtitle:(TOPIC_MAP[iq.topicId]?.title||'')+' · '+q.difficulty, text:q.a }))),
    ...CODE_SNIPPETS.flatMap(cs=>cs.snippets.map(s=>({ type:'snippet', id:cs.topicId, nav:'snippets', title:s.title, subtitle:s.language.toUpperCase()+' · '+(TOPIC_MAP[cs.topicId]?.title||''), text:s.explanation })))
  ];
}

function openSearch() {
  const overlay = document.getElementById('search-overlay');
  overlay.classList.add('visible');
  overlay.removeAttribute('aria-hidden');
  setTimeout(() => document.getElementById('search-field').focus(), 50);
}
function closeSearch() {
  const overlay = document.getElementById('search-overlay');
  overlay.classList.remove('visible');
  overlay.setAttribute('aria-hidden', 'true');
  document.getElementById('search-field').value = '';
  document.getElementById('search-results').innerHTML = '';
}

// Focus trap inside search modal
document.addEventListener('keydown', e => {
  const overlay = document.getElementById('search-overlay');
  if (!overlay || !overlay.classList.contains('visible')) return;
  if (e.key !== 'Tab') return;
  const focusable = overlay.querySelectorAll('input, button, [tabindex]:not([tabindex="-1"])');
  const first = focusable[0], last = focusable[focusable.length - 1];
  if (e.shiftKey) { if (document.activeElement === first) { e.preventDefault(); last.focus(); } }
  else            { if (document.activeElement === last)  { e.preventDefault(); first.focus(); } }
});
function doSearch(query) {
  const q = query.trim().toLowerCase();
  const el = document.getElementById('search-results');
  if (!q) { el.innerHTML=''; return; }
  const matches = searchIndex.filter(item=>(item.title+' '+item.subtitle+' '+item.text).toLowerCase().includes(q)).slice(0,12);
  if (!matches.length) { el.innerHTML=`<div class="search-empty">No results for "${query}"</div>`; return; }
  const hi = t => t.replace(new RegExp(`(${escapeRegex(query)})`, 'gi'),'<mark>$1</mark>');
  el.innerHTML = matches.map(m=>`
    <div class="search-result-item" data-nav="${m.nav}" data-id="${m.id}">
      <span class="sr-type sr-type-${m.type}">${m.type}</span>
      <div><div class="sr-title">${hi(m.title)}</div><div class="sr-subtitle">${m.subtitle}</div></div>
    </div>`).join('');
  el.querySelectorAll('.search-result-item').forEach(item=>{
    item.addEventListener('click',()=>{
      navigate(item.dataset.nav); closeSearch();
      setTimeout(()=>{
        const card = document.querySelector(`[data-id="${item.dataset.id}"]`);
        if (card) { card.classList.add('expanded'); card.scrollIntoView({behavior:'smooth',block:'start'}); }
      },200);
    });
  });
}
function escapeRegex(s) { return s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'); }

// ---- GAP FILTER ----
function setupGapFilters() {
  document.querySelectorAll('.filter-btn[data-filter]').forEach(btn=>{
    btn.addEventListener('click',()=>{
      document.querySelectorAll('.filter-btn[data-filter]').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      buildGaps(btn.dataset.filter);
    });
  });
}

// ---- SQL TIER FILTER ----
function setupSqlFilters() {
  document.querySelectorAll('.filter-btn[data-sql-filter]').forEach(btn=>{
    btn.addEventListener('click',()=>{
      document.querySelectorAll('.filter-btn[data-sql-filter]').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      buildSqlFundamentals(btn.dataset.sqlFilter);
    });
  });
}

// ---- ROADMAP ----
function isTopicDone(phaseId, idx) { return localStorage.getItem(`mk_rm_${phaseId}_${idx}`) === '1'; }
function setTopicDone(phaseId, idx, v) { localStorage.setItem(`mk_rm_${phaseId}_${idx}`, v ? '1' : '0'); }

function buildRoadmap() {
  const el = document.getElementById('roadmap-content');
  if (!el || typeof ROADMAP_PHASES === 'undefined') return;
  el.innerHTML = ROADMAP_PHASES.map(p => {
    const done = p.topics.filter((_, i) => isTopicDone(p.id, i)).length;
    const total = p.topics.length;
    const pct = total ? Math.round(done / total * 100) : 0;
    return `
    <div class="roadmap-phase phase-color-${p.color}" data-phase-id="${p.id}">
      <div class="roadmap-phase-header">
        <div class="roadmap-phase-num">Phase ${p.phase}</div>
        <div class="roadmap-phase-emoji">${p.emoji}</div>
        <div class="roadmap-phase-info">
          <div class="roadmap-phase-title">${p.title}</div>
          <div class="roadmap-phase-subtitle">${p.subtitle}</div>
        </div>
        <div class="roadmap-phase-meta">
          <span class="roadmap-time">${p.timeEstimate}</span>
          <span class="roadmap-priority roadmap-priority-${p.priority}">${p.priority}</span>
        </div>
      </div>
      <div class="roadmap-progress-bar-wrap">
        <div class="roadmap-progress-label">${done}/${total} topics done</div>
        <div class="roadmap-progress-track"><div class="roadmap-progress-fill" style="width:${pct}%"></div></div>
      </div>
      <p class="roadmap-desc">${p.description}</p>
      <div class="roadmap-topics">
        <div class="roadmap-topics-label">Topics</div>
        <ul class="roadmap-topics-list">${p.topics.map((t, i) => {
          const checked = isTopicDone(p.id, i);
          return `<li class="roadmap-topic-item ${checked?'topic-done':''}">
            <label>
              <input type="checkbox" class="roadmap-cb" data-phase="${p.id}" data-idx="${i}" ${checked?'checked':''}>
              <span>${t}</span>
            </label>
          </li>`;
        }).join('')}</ul>
      </div>
      <div class="roadmap-resources">
        <div class="roadmap-topics-label">Resources</div>
        <div class="roadmap-resource-links">${p.resources.map(r=>`<a href="${r.url}" target="_blank" rel="noopener" class="roadmap-link">${r.name}</a>`).join('')}</div>
      </div>
      <div class="roadmap-checkpoint">
        <span class="checkpoint-icon">✅</span>
        <span><strong>Checkpoint:</strong> ${p.checkpoint}</span>
      </div>
      ${p.linkedSection ? `<button class="roadmap-goto-btn" onclick="navigate('${p.linkedSection}')">Go to ${p.title} →</button>` : ''}
    </div>`;
  }).join('');

  el.querySelectorAll('.roadmap-cb').forEach(cb => {
    cb.addEventListener('change', () => {
      const phaseId = cb.dataset.phase;
      const idx = parseInt(cb.dataset.idx);
      setTopicDone(phaseId, idx, cb.checked);
      const li = cb.closest('.roadmap-topic-item');
      if (li) li.classList.toggle('topic-done', cb.checked);
      // Update progress bar
      const phaseEl = cb.closest('.roadmap-phase');
      const allCbs = phaseEl.querySelectorAll('.roadmap-cb');
      const doneCount = [...allCbs].filter(c => c.checked).length;
      const pct = Math.round(doneCount / allCbs.length * 100);
      const fill = phaseEl.querySelector('.roadmap-progress-fill');
      const label = phaseEl.querySelector('.roadmap-progress-label');
      if (fill) fill.style.width = pct + '%';
      if (label) label.textContent = `${doneCount}/${allCbs.length} topics done`;
    });
  });
}

// ---- SQL FUNDAMENTALS ----
function buildSqlFundamentals(filter) {
  const el = document.getElementById('sql-content');
  if (!el || typeof SQL_TOPICS === 'undefined') return;
  const topics = filter && filter !== 'all'
    ? SQL_TOPICS.filter(t => String(t.tier) === String(filter))
    : SQL_TOPICS;
  el.innerHTML = topics.map(t => `
    <div class="sql-topic-card tier-${t.tier} ${isSqlSolved(t.id)?'sql-solved':''}"
      <div class="sql-topic-header">
        <span class="sql-tier-badge tier-badge-${t.tier}">${t.tierLabel}</span>
        <span class="sql-topic-title">${t.title}</span>
      </div>
      <p class="sql-summary">${t.summary}</p>
      <div class="sql-explanation">${t.explanation}</div>
      <div class="sql-example">
        <div class="sql-label">Example</div>
        <pre><code class="language-sql">${escHtml(t.example)}</code></pre>
      </div>
      <div class="sql-practice">
        <div class="sql-label">Practice Problem</div>
        <p class="sql-practice-text">${t.practice}</p>
        <div class="sql-practice-actions">
          <button class="sql-reveal-btn" onclick="toggleSqlAnswer(this)">Show Answer</button>
          <button class="sql-solved-btn ${isSqlSolved(t.id)?'solved':''}" data-id="${t.id}" onclick="toggleSqlSolved(this)">
            ${isSqlSolved(t.id)?'✓ Solved':'Mark as Solved'}
          </button>
        </div>
        <div class="sql-answer hidden">
          <pre><code class="language-sql">${escHtml(t.answer)}</code></pre>
        </div>
      </div>
    </div>
  `).join('');
  prismHighlight(el);
}
function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function isSqlSolved(id)   { return localStorage.getItem(`mk_sql_${id}`) === '1'; }
function setSqlSolved(id,v){ localStorage.setItem(`mk_sql_${id}`, v ? '1' : '0'); }
function toggleSqlAnswer(btn) {
  const ans = btn.closest('.sql-practice').querySelector('.sql-answer');
  if (ans.classList.contains('hidden')) {
    ans.classList.remove('hidden');
    btn.textContent = 'Hide Answer';
  } else {
    ans.classList.add('hidden');
    btn.textContent = 'Show Answer';
  }
}
function toggleSqlSolved(btn) {
  const id = btn.dataset.id;
  const solved = !isSqlSolved(id);
  setSqlSolved(id, solved);
  btn.classList.toggle('solved', solved);
  btn.textContent = solved ? '✓ Solved' : 'Mark as Solved';
  const card = btn.closest('.sql-topic-card');
  if (card) card.classList.toggle('sql-solved', solved);
}

// ---- PYTHON FOR DE ----
function buildPythonDE() {
  const el = document.getElementById('python-content');
  if (!el || typeof PYTHON_MODULES === 'undefined') return;
  el.innerHTML = PYTHON_MODULES.map(m => `
    <div class="py-module-card">
      <div class="py-module-header">
        <span class="py-module-num">Module ${m.module}</span>
        <div class="py-module-info">
          <div class="py-module-title">${m.title}</div>
          <div class="py-module-subtitle">${m.subtitle}</div>
        </div>
      </div>
      <p class="py-summary">${m.summary}</p>
      <ul class="py-points">${m.points.map(p=>`<li>${p}</li>`).join('')}</ul>
      <div class="py-code-block">
        <div class="py-code-label">Code Example</div>
        <pre><code class="language-python">${escHtml(m.code)}</code></pre>
      </div>
    </div>
  `).join('');
  prismHighlight(el);
}

// ---- SETUP GUIDES ----
function buildSetupGuides() {
  const el = document.getElementById('guides-content');
  if (!el || typeof SETUP_GUIDES === 'undefined') return;
  let html = '<div class="guides-grid">';
  html += SETUP_GUIDES.map(g => `
    <div class="guide-card" id="guide-${g.id}">
      <div class="guide-header">
        <span class="guide-icon">${g.icon}</span>
        <div>
          <div class="guide-title">${g.title}</div>
          <div class="guide-subtitle">${g.subtitle}</div>
        </div>
      </div>
      <div class="guide-steps">
        ${g.steps.map((s,i) => `
          <div class="guide-step">
            <div class="guide-step-num">${i+1}</div>
            <div class="guide-step-body">
              <div class="guide-step-title">${s.title}</div>
              ${s.commands && s.commands.length ? `<pre class="guide-commands"><code class="language-bash">${s.commands.map(c=>escHtml(c)).join('\n')}</code></pre>` : ''}
              ${s.note ? `<div class="guide-note">${s.note}</div>` : ''}
            </div>
          </div>
        `).join('')}
      </div>
    </div>
  `).join('');
  html += '</div>';
  el.innerHTML = html;
  prismHighlight(el);
}

// ---- GUIDED PROJECTS ----
function buildProjects() {
  const el = document.getElementById('projects-content');
  if (!el || typeof GUIDED_PROJECTS === 'undefined') return;
  el.innerHTML = GUIDED_PROJECTS.map(p => `
    <div class="project-card">
      <div class="project-header">
        <span class="project-emoji">${p.emoji}</span>
        <div class="project-info">
          <div class="project-title">Project ${p.number}: ${p.title}</div>
          <div class="project-meta">
            <span class="project-difficulty difficulty-${p.difficulty.toLowerCase()}">${p.difficulty}</span>
            <span class="project-time">${p.time}</span>
          </div>
        </div>
      </div>
      <p class="project-desc">${p.description}</p>
      <div class="project-prereqs">
        <strong>Prerequisites:</strong> ${p.prerequisites.join(', ')}
      </div>
      <div class="project-teaches">
        <strong>You'll learn:</strong> ${p.teaches.join(' · ')}
      </div>
      <div class="project-architecture">
        <div class="project-arch-label">Architecture</div>
        <div class="project-arch-flow">${p.architecture.map(a=>`<span class="arch-step">${a}</span>`).join('<span class="arch-arrow">→</span>')}</div>
      </div>
      <div class="project-steps">
        ${p.steps.map((s,i) => `
          <div class="project-step">
            <div class="project-step-header">
              <span class="project-step-num">Step ${i+1}</span>
              <span class="project-step-title">${s.title}</span>
            </div>
            <p class="project-step-detail">${s.detail}</p>
            ${s.code ? `<pre class="project-code"><code class="language-python">${escHtml(s.code)}</code></pre>` : ''}
          </div>
        `).join('')}
      </div>
    </div>
  `).join('');
  prismHighlight(el);
}

// ---- DE EXPERT BOT ----
// ---- MARKDOWN RENDERER (shared by bot + IQ answers) ----
function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function inlineMarkdown(text) {
  return text
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code class="bot-inline-code">$1</code>');
}

function renderTable(lines) {
  const rows = lines.filter(l => !/^\|[-|\s:]+\|$/.test(l.trim()));
  if (rows.length === 0) return '';
  let html = '<div class="bot-table-wrap"><table class="bot-table">';
  rows.forEach((row, idx) => {
    const cells = row.split('|').filter((_, ci) => ci > 0 && ci < row.split('|').length - 1);
    const tag = idx === 0 ? 'th' : 'td';
    html += '<tr>' + cells.map(c => `<${tag}>${inlineMarkdown(c.trim())}</${tag}>`).join('') + '</tr>';
  });
  html += '</table></div>';
  return html;
}

function renderMarkdown(text) {
  const codeBlocks = [];
  text = text.replace(/```(\w*)\n?([\s\S]*?)```/g, (_, lang, code) => {
    const idx = codeBlocks.length;
    const cls = lang ? ` class="language-${lang}"` : '';
    codeBlocks.push(`<pre class="bot-code"><code${cls}>${escapeHtml(code.trim())}</code></pre>`);
    return `\x00CODE${idx}\x00`;
  });

  const lines = text.split('\n');
  const out = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    if (/^\x00CODE\d+\x00$/.test(line.trim())) {
      const idx = parseInt(line.trim().replace(/\x00CODE(\d+)\x00/, '$1'));
      out.push(codeBlocks[idx]);
      i++;
      continue;
    }

    if (/^(-{3,}|={3,})$/.test(line.trim())) {
      out.push('<hr class="bot-hr">');
      i++;
      continue;
    }

    const headM = line.match(/^(#{1,3})\s+(.+)$/);
    if (headM) {
      const lvl = headM[1].length + 2;
      out.push(`<h${lvl} class="bot-heading">${inlineMarkdown(headM[2])}</h${lvl}>`);
      i++;
      continue;
    }

    if (line.trim().startsWith('|')) {
      const tableLines = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) { tableLines.push(lines[i]); i++; }
      out.push(renderTable(tableLines));
      continue;
    }

    if (/^(\s*[-*])\s/.test(line)) {
      const listItems = [];
      while (i < lines.length && /^(\s*[-*])\s/.test(lines[i])) {
        listItems.push(`<li>${inlineMarkdown(lines[i].replace(/^\s*[-*]\s/, ''))}</li>`);
        i++;
      }
      out.push(`<ul class="bot-list">${listItems.join('')}</ul>`);
      continue;
    }

    if (/^\d+\.\s/.test(line)) {
      const listItems = [];
      while (i < lines.length && /^\d+\.\s/.test(lines[i])) {
        listItems.push(`<li>${inlineMarkdown(lines[i].replace(/^\d+\.\s/, ''))}</li>`);
        i++;
      }
      out.push(`<ol class="bot-list">${listItems.join('')}</ol>`);
      continue;
    }

    if (line.trim() === '') { i++; continue; }
    out.push(`<p class="bot-p">${inlineMarkdown(line)}</p>`);
    i++;
  }

  return out.join('');
}

function buildBot() {
  const messagesEl = document.getElementById('bot-messages');
  const inputEl    = document.getElementById('bot-input');
  const sendBtn    = document.getElementById('bot-send-btn');
  if (!messagesEl || !inputEl || !sendBtn) return;

  function addCopyButtons(container) {
    container.querySelectorAll('pre.bot-code').forEach(pre => {
      if (pre.querySelector('.bot-copy-btn')) return;
      const btn = document.createElement('button');
      btn.className = 'bot-copy-btn';
      btn.textContent = 'Copy';
      btn.addEventListener('click', () => {
        const code = pre.querySelector('code');
        navigator.clipboard.writeText(code ? code.innerText : pre.innerText).then(() => {
          btn.textContent = 'Copied!';
          setTimeout(() => { btn.textContent = 'Copy'; }, 1800);
        }).catch(() => {
          const sel = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(code || pre);
          sel.removeAllRanges();
          sel.addRange(range);
          btn.textContent = 'Selected!';
          setTimeout(() => { btn.textContent = 'Copy'; }, 1800);
        });
      });
      pre.style.position = 'relative';
      pre.appendChild(btn);
    });
  }

  function updateSuggestions(relatedText) {
    const suggestionsEl = document.getElementById('bot-suggestions');
    if (!suggestionsEl) return;
    const matches = relatedText ? [...relatedText.matchAll(/\*\*(.+?)\*\*/g)].map(m => m[1]) : [];
    if (matches.length > 0) {
      suggestionsEl.innerHTML = matches.slice(0, 6).map(t =>
        `<span class="bot-sugg" data-q="Explain ${t}">${t}</span>`
      ).join('');
      suggestionsEl.querySelectorAll('.bot-sugg').forEach(el => {
        el.addEventListener('click', () => { inputEl.value = el.dataset.q; handleSend(); });
      });
    }
  }

  const COLLAPSE_THRESHOLD = 900; // chars — answers longer than this get a "Show more"

  function addMessage(text, role) {
    const wrap = document.createElement('div');
    wrap.className = `bot-msg bot-msg-${role}`;
    const bubble = document.createElement('div');
    bubble.className = 'bot-bubble';

    if (role === 'bot' && text.length > COLLAPSE_THRESHOLD) {
      const inner = document.createElement('div');
      inner.className = 'bot-bubble-inner bot-bubble-collapsed';
      inner.innerHTML = renderMarkdown(text);
      const toggle = document.createElement('button');
      toggle.className = 'bot-expand-btn';
      toggle.textContent = 'Show more ▾';
      toggle.addEventListener('click', () => {
        const collapsed = inner.classList.toggle('bot-bubble-collapsed');
        toggle.textContent = collapsed ? 'Show more ▾' : 'Show less ▴';
        if (!collapsed) messagesEl.scrollTop = wrap.offsetTop - 20;
      });
      bubble.appendChild(inner);
      bubble.appendChild(toggle);
      addCopyButtons(inner);
    } else {
      bubble.innerHTML = renderMarkdown(text);
      if (role === 'bot') addCopyButtons(bubble);
    }

    wrap.appendChild(bubble);
    messagesEl.appendChild(wrap);
    if (role === 'bot') updateSuggestions(text);
    // Only auto-scroll if user is near bottom (within 120px)
    const nearBottom = messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < 120;
    if (nearBottom || role === 'user') messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function addTyping() {
    const wrap = document.createElement('div');
    wrap.className = 'bot-msg bot-msg-bot';
    wrap.id = 'bot-typing';
    wrap.innerHTML = '<div class="bot-bubble bot-typing-indicator"><span></span><span></span><span></span></div>';
    messagesEl.appendChild(wrap);
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function removeTyping() {
    const el = document.getElementById('bot-typing');
    if (el) el.remove();
  }

  function handleSend() {
    const val = inputEl.value.trim();
    if (!val) return;
    addMessage(val, 'user');
    inputEl.value = '';
    addTyping();
    // Scale delay by estimated response length (short = fast, long = slightly slower)
    const delay = 350 + Math.min(val.length * 4, 500);
    setTimeout(() => {
      removeTyping();
      const response = botRespond(val);
      addMessage(response, 'bot');
    }, delay);
  }

  sendBtn.addEventListener('click', handleSend);
  inputEl.addEventListener('keydown', e => { if (e.key === 'Enter') handleSend(); });

  const clearBtn = document.getElementById('bot-clear-btn');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      messagesEl.innerHTML = '';
      // Reset all bot memory state
      BOT_LAST_TOPIC = null;
      BOT_LAST_ENTRY = null;
      BOT_HISTORY.splice(0);
      BOT_EXCHANGE.splice(0);
      Object.keys(BOT_ASPECTS).forEach(k => delete BOT_ASPECTS[k]);
      BOT_QUIZ_PENDING = null;
      BOT_QUIZ_IDX = null;
      BOT_QUIZ_RECENT.clear();
      // Reset suggestion chips to defaults
      const suggestionsEl = document.getElementById('bot-suggestions');
      if (suggestionsEl) {
        suggestionsEl.innerHTML = [
          ['What is data engineering?','What is DE?'],
          ['Explain window functions','Window functions'],
          ['ETL vs ELT difference','ETL vs ELT'],
          ['How do I connect Python to PostgreSQL?','Python → PostgreSQL'],
          ['What is idempotency?','Idempotency'],
          ['Explain Airflow DAG','Airflow DAG'],
          ['How to prepare for a data engineer interview?','Interview tips'],
          ['What is a star schema?','Star schema'],
          ['Quiz me on SQL','Quiz: SQL'],
          ['Quiz me on data engineering','Quiz: DE concepts'],
        ].map(([q,label]) => `<span class="bot-sugg" data-q="${q}">${label}</span>`).join('');
        suggestionsEl.querySelectorAll('.bot-sugg').forEach(el => {
          el.addEventListener('click', () => { inputEl.value = el.dataset.q; handleSend(); });
        });
      }
      addMessage('Chat cleared. Ask me anything about Data Engineering, SQL, Python, or tools.', 'bot');
    });
  }

  document.querySelectorAll('.bot-sugg').forEach(el => {
    el.addEventListener('click', () => {
      inputEl.value = el.dataset.q;
      handleSend();
    });
  });

  // Welcome message
  addMessage(`Hey! I'm your offline DE Expert.\n\nI know everything in this platform — SQL, Python, Airflow, Spark, dbt, pipeline design, and interview prep.\n\nAsk me anything or pick a suggestion below.`, 'bot');
}

// ---- INIT ----
document.addEventListener('DOMContentLoaded', ()=>{
  buildBot();
  buildSearchIndex();
  buildHeroStats();
  buildDailyRec();
  updateSidebarScore();
  recordActivity();
  buildStreakBadge();

  document.querySelectorAll('.nav-item').forEach(item=>
    item.addEventListener('click',()=>navigate(item.dataset.nav)));

  document.getElementById('search-trigger-btn')?.addEventListener('click',openSearch);
  document.getElementById('search-overlay')?.addEventListener('click',e=>{ if(e.target===e.currentTarget) closeSearch(); });
  document.getElementById('search-close-btn')?.addEventListener('click',closeSearch);
  document.getElementById('search-field')?.addEventListener('input',e=>doSearch(e.target.value));

  document.addEventListener('keydown',e=>{
    if ((e.ctrlKey||e.metaKey) && e.key==='k') { e.preventDefault(); openSearch(); }
    if (e.key==='Escape') closeSearch();
  });

  navigate('home');

  // ---- ONBOARDING ----
  if (!localStorage.getItem('mk_onboarded')) {
    const overlay = document.getElementById('onboarding-overlay');
    const slides  = document.querySelectorAll('.onboarding-slide');
    const dots    = document.querySelectorAll('.onboarding-dot');
    const prevBtn = document.getElementById('onboarding-prev');
    const nextBtn = document.getElementById('onboarding-next');
    let cur = 0;

    function goSlide(n) {
      slides[cur].classList.remove('active');
      dots[cur].classList.remove('active');
      cur = n;
      slides[cur].classList.add('active');
      dots[cur].classList.add('active');
      prevBtn.style.visibility = cur === 0 ? 'hidden' : 'visible';
      nextBtn.textContent = cur === slides.length - 1 ? 'Get Started!' : 'Next →';
    }

    function closeOnboarding() {
      overlay.classList.remove('visible');
      localStorage.setItem('mk_onboarded', '1');
    }

    if (overlay) {
      overlay.classList.add('visible');
      prevBtn?.addEventListener('click', () => { if (cur > 0) goSlide(cur - 1); });
      nextBtn?.addEventListener('click', () => {
        if (cur < slides.length - 1) goSlide(cur + 1);
        else closeOnboarding();
      });
      overlay.addEventListener('click', e => { if (e.target === overlay) closeOnboarding(); });
    }
  }

  // ---- MOBILE SIDEBAR TOGGLE ----
  const menuBtn     = document.getElementById('mobile-menu-btn');
  const sidebarEl   = document.getElementById('sidebar');
  const overlayEl   = document.getElementById('sidebar-overlay');

  function openSidebar() {
    sidebarEl.classList.add('open');
    overlayEl.classList.add('visible');
    menuBtn.classList.add('open');
  }
  function closeSidebar() {
    sidebarEl.classList.remove('open');
    overlayEl.classList.remove('visible');
    menuBtn.classList.remove('open');
  }

  menuBtn?.addEventListener('click', () =>
    sidebarEl.classList.contains('open') ? closeSidebar() : openSidebar()
  );
  overlayEl?.addEventListener('click', closeSidebar);

  // Close sidebar when nav item tapped on mobile
  document.querySelectorAll('.nav-item').forEach(item =>
    item.addEventListener('click', () => {
      if (window.innerWidth <= 768) closeSidebar();
    })
  );
});
