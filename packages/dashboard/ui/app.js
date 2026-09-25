// Meridian dashboard. Plain ES modules, no build step.
// All dynamic content goes through textContent (see h()), never innerHTML,
// because job payloads are user data.

const STATES = ['waiting', 'active', 'delayed', 'completed', 'failed'];
const PAGE_SIZE = 20;
const REFRESH_MS = 3_000;

const state = {
  overview: null,
  queue: localGet('queue'),
  jobState: localGet('jobState') ?? 'failed',
  page: 0,
  total: 0,
  /** Last rendered job list, to skip re-rendering identical data (keeps hover and focus). */
  jobsKey: '',
};

// ---------- helpers ----------

function localGet(key) {
  try {
    return localStorage.getItem(`meridian:${key}`);
  } catch {
    return null;
  }
}

function localSet(key, value) {
  try {
    localStorage.setItem(`meridian:${key}`, value);
  } catch {
    // Private mode or blocked storage: the preference just is not remembered.
  }
}

/** Creates an element. Strings become text nodes, so they are never parsed as HTML. */
function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key.startsWith('on')) el.addEventListener(key.slice(2), value);
    else if (key === 'class') el.className = value;
    else el.setAttribute(key, value === true ? '' : String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

function svg(tag, attrs = {}) {
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, String(value));
  return el;
}

const $ = (id) => document.getElementById(id);
const number = new Intl.NumberFormat();

function duration(ms) {
  if (!ms) return '—';
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function ago(timestamp) {
  if (!timestamp) return '—';
  const seconds = Math.round((Date.now() - timestamp) / 1_000);
  if (seconds < 60) return `${Math.max(seconds, 0)}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h ago`;
  return new Date(timestamp).toLocaleString();
}

function toast(message) {
  const el = $('toast');
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => {
    el.hidden = true;
  }, 2_500);
}

async function api(path, options = {}) {
  const res = await fetch(`api/${path}`, {
    ...options,
    headers: { 'x-meridian-request': '1', ...options.headers },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error ?? `Request failed (${res.status})`);
  return body;
}

async function action(label, path, method = 'POST') {
  try {
    await api(path, { method });
    toast(label);
    await refresh();
  } catch (err) {
    toast(err.message);
  }
}

const q = (name) => encodeURIComponent(name);

// ---------- overview ----------

function renderStats({ totals }) {
  const stats = [
    ['Jobs / min', totals.jobsPerMinute],
    ['Waiting', totals.waiting],
    ['Active', totals.active],
    ['Delayed', totals.delayed],
    ['Failed', totals.failed, totals.failed > 0],
    ['Processes', totals.processes],
  ];
  $('stats').replaceChildren(
    ...stats.map(([label, value, alert]) =>
      h(
        'div',
        { class: alert ? 'stat alert' : 'stat' },
        h('div', { class: 'label' }, label),
        h('div', { class: 'value' }, number.format(value)),
      ),
    ),
  );
}

function renderQueues({ queues }) {
  $('queues-empty').hidden = queues.length > 0;
  $('queues').replaceChildren(
    ...queues.map((queue) =>
      h(
        'tr',
        {},
        h(
          'td',
          {},
          h('span', { class: 'queue-name' }, queue.name),
          queue.paused && h('span', { class: 'badge warn' }, 'paused'),
        ),
        h('td', { class: 'num' }, number.format(queue.counts.waiting)),
        h('td', { class: 'num' }, number.format(queue.counts.active)),
        h('td', { class: 'num' }, number.format(queue.counts.delayed)),
        h(
          'td',
          { class: queue.counts.failed > 0 ? 'num bad' : 'num' },
          number.format(queue.counts.failed),
        ),
        h('td', { class: 'num' }, queue.jobsPerMinute),
        h('td', { class: 'num' }, duration(queue.avgRuntime)),
        h('td', { class: 'num' }, duration(queue.avgWait)),
        h(
          'td',
          { class: 'num' },
          h(
            'button',
            {
              class: 'secondary',
              onclick: () =>
                queue.paused
                  ? action(`Resumed ${queue.name}`, `queues/${q(queue.name)}/resume`)
                  : action(`Paused ${queue.name}`, `queues/${q(queue.name)}/pause`),
            },
            queue.paused ? 'Resume' : 'Pause',
          ),
        ),
      ),
    ),
  );
}

function renderSupervisors({ supervisors }) {
  const root = $('supervisors');
  if (supervisors.length === 0) {
    root.replaceChildren(
      h('p', { class: 'empty' }, 'No supervisor is running. Start one with meridian-supervisor.'),
    );
    return;
  }
  root.replaceChildren(
    ...supervisors.map((s) =>
      h(
        'div',
        { class: 'supervisor' },
        h('strong', {}, s.name),
        h('span', { class: 'badge' }, s.strategy),
        h('div', { class: 'muted' }, `${s.host} · pid ${s.pid} · up since ${ago(s.startedAt)}`),
        h(
          'div',
          { class: 'pools' },
          Object.entries(s.queues).map(([queue, pool]) =>
            h(
              'span',
              { class: 'pool', title: `target ${pool.target}` },
              `${queue}: ${pool.processes}`,
            ),
          ),
        ),
      ),
    ),
  );
}

function renderChart(buckets) {
  const root = $('chart');
  const width = 600;
  const height = 160;
  const max = Math.max(1, ...buckets.map((b) => b.completed + b.failed));
  const step = width / Math.max(buckets.length, 1);
  const barWidth = Math.max(step - 2, 1);

  const chart = svg('svg', { viewBox: `0 0 ${width} ${height}`, preserveAspectRatio: 'none' });
  buckets.forEach((bucket, i) => {
    const okHeight = (bucket.completed / max) * (height - 4);
    const badHeight = (bucket.failed / max) * (height - 4);
    const x = i * step + 1;
    const group = svg('g');
    const title = svg('title');
    title.textContent = `${new Date(bucket.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}: ${bucket.completed} completed, ${bucket.failed} failed`;
    group.append(title);
    group.append(
      svg('rect', {
        class: 'bar ok',
        x,
        y: height - okHeight,
        width: barWidth,
        height: okHeight,
        rx: 1,
      }),
    );
    group.append(
      svg('rect', {
        class: 'bar bad',
        x,
        y: height - okHeight - badHeight,
        width: barWidth,
        height: badHeight,
        rx: 1,
      }),
    );
    // Invisible full-height target so the tooltip works on empty minutes too.
    group.append(svg('rect', { x, y: 0, width: barWidth, height, fill: 'transparent' }));
    chart.append(group);
  });
  root.replaceChildren(chart);
}

function syncQueueSelect({ queues }) {
  const select = $('job-queue');
  const names = queues.map((queue) => queue.name);
  if (!names.includes(state.queue)) state.queue = names[0] ?? null;
  const current = [...select.options].map((o) => o.value).join('\n');
  if (current !== names.join('\n')) {
    select.replaceChildren(...names.map((name) => h('option', { value: name }, name)));
  }
  if (state.queue) select.value = state.queue;
}

// ---------- jobs ----------

function renderTabs() {
  const queue = state.overview?.queues.find((x) => x.name === state.queue);
  $('job-tabs').replaceChildren(
    ...STATES.map((name) =>
      h(
        'button',
        {
          role: 'tab',
          'aria-selected': String(name === state.jobState),
          onclick: () => {
            state.jobState = name;
            state.page = 0;
            localSet('jobState', name);
            renderTabs();
            void loadJobs();
          },
        },
        name,
        queue && h('span', { class: 'count' }, number.format(queue.counts[name] ?? 0)),
      ),
    ),
  );
  $('retry-all').hidden = !(state.jobState === 'failed' && (queue?.counts.failed ?? 0) > 0);
}

function jobTime(job) {
  if (state.jobState === 'delayed') return `created ${ago(job.timestamp)}`;
  return ago(job.finishedOn ?? job.processedOn ?? job.timestamp);
}

async function loadJobs() {
  const list = $('jobs');
  if (!state.queue) {
    list.replaceChildren(h('li', { class: 'empty' }, 'No queue selected'));
    return;
  }
  try {
    const result = await api(
      `queues/${q(state.queue)}/jobs?state=${state.jobState}&page=${state.page}&size=${PAGE_SIZE}`,
    );
    state.total = result.total;
    const key = JSON.stringify([state.queue, state.jobState, result]);
    if (key === state.jobsKey) return;
    state.jobsKey = key;
    if (result.jobs.length === 0) {
      list.replaceChildren(h('p', { class: 'empty' }, `No ${state.jobState} jobs`));
    } else {
      list.replaceChildren(
        ...result.jobs.map((job) =>
          h(
            'li',
            {
              onclick: () => void openJob(job.id),
              tabindex: 0,
              onkeydown: (e) => e.key === 'Enter' && openJob(job.id),
            },
            h(
              'div',
              { class: 'main' },
              h('div', { class: 'title' }, `#${job.id} `, job.name),
              h(
                'div',
                { class: job.failedReason && state.jobState === 'failed' ? 'sub reason' : 'sub' },
                state.jobState === 'failed' ? job.failedReason : JSON.stringify(job.data),
              ),
            ),
            job.attemptsMade > 1 && h('span', { class: 'badge' }, `${job.attemptsMade} attempts`),
            h('span', { class: 'muted' }, jobTime(job)),
          ),
        ),
      );
    }
    const pages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
    $('page-label').textContent = `Page ${state.page + 1} of ${pages}`;
    $('prev').disabled = state.page === 0;
    $('next').disabled = state.page + 1 >= pages;
  } catch (err) {
    state.jobsKey = '';
    list.replaceChildren(h('p', { class: 'empty' }, err.message));
  }
}

function section(title, value, className) {
  if (value === undefined || value === null || value === '') return null;
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  return [h('h3', { class: 'muted' }, title), h('pre', { class: className }, text)];
}

async function openJob(id) {
  const dialog = $('job-dialog');
  try {
    const job = await api(`queues/${q(state.queue)}/jobs/${q(id)}`);
    $('job-title').textContent = `#${job.id} ${job.name}`;

    const meta = [
      [
        'State',
        h(
          'span',
          {
            class: `badge ${job.state === 'failed' ? 'bad' : job.state === 'completed' ? 'ok' : ''}`,
          },
          job.state,
        ),
      ],
      ['Queue', state.queue],
      ['Created', new Date(job.timestamp).toLocaleString()],
      ['Started', job.processedOn ? new Date(job.processedOn).toLocaleString() : '—'],
      ['Finished', job.finishedOn ? new Date(job.finishedOn).toLocaleString() : '—'],
      [
        'Runtime',
        job.finishedOn && job.processedOn ? duration(job.finishedOn - job.processedOn) : '—',
      ],
      ['Attempts', `${job.attemptsMade} of ${job.opts.attempts ?? 1}`],
    ];
    $('job-meta').replaceChildren(
      ...meta.flatMap(([label, value]) => [h('dt', {}, label), h('dd', {}, value)]),
    );

    $('job-sections').replaceChildren(
      ...[
        section('Data', job.data),
        section('Result', job.returnValue),
        section('Error', job.failedReason, 'error'),
        section('Stack trace', job.stacktrace, 'error'),
        section('Options', job.opts),
      ]
        .filter(Boolean)
        .flat(),
    );

    const close = () => dialog.close();
    $('job-actions').replaceChildren(
      job.state === 'failed' &&
        h(
          'button',
          {
            onclick: () => {
              close();
              void action(
                `Retrying #${job.id}`,
                `queues/${q(state.queue)}/jobs/${q(job.id)}/retry`,
              );
            },
          },
          'Retry',
        ),
      job.state !== 'active' &&
        h(
          'button',
          {
            class: 'danger',
            onclick: () => {
              if (!confirm(`Delete job #${job.id}? This cannot be undone.`)) return;
              close();
              void action(
                `Deleted #${job.id}`,
                `queues/${q(state.queue)}/jobs/${q(job.id)}`,
                'DELETE',
              );
            },
          },
          'Delete',
        ),
    );
    dialog.showModal();
  } catch (err) {
    toast(err.message);
  }
}

// ---------- live updates ----------

function addEvent(event) {
  const feed = $('feed');
  const time = new Date(Number(event.id.split('-')[0])).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const detail = [event.queue, event.jobId && `#${event.jobId}`, event.name, event.reason]
    .filter(Boolean)
    .join(' · ');
  feed.prepend(
    h(
      'li',
      {},
      h('time', {}, time),
      h('span', { class: `event-${event.event}` }, event.event),
      h('span', {}, detail),
    ),
  );
  while (feed.children.length > 100) feed.lastChild.remove();
}

function connectEvents() {
  const live = $('live');
  const source = new EventSource('api/events');
  let refreshTimer = null;
  source.onopen = () => {
    live.classList.add('on');
    $('live-label').textContent = 'live';
  };
  source.onerror = () => {
    live.classList.remove('on');
    $('live-label').textContent = 'reconnecting…';
  };
  source.onmessage = (message) => {
    addEvent(JSON.parse(message.data));
    // Coalesce bursts of events into one refresh.
    if (refreshTimer === null) {
      refreshTimer = setTimeout(() => {
        refreshTimer = null;
        void refresh();
      }, 500);
    }
  };
}

// ---------- main ----------

let refreshing;
async function refresh() {
  refreshing ??= (async () => {
    try {
      const [overview, metrics] = await Promise.all([api('overview'), api('metrics?minutes=60')]);
      state.overview = overview;
      renderStats(overview);
      renderQueues(overview);
      renderSupervisors(overview);
      renderChart(metrics);
      syncQueueSelect(overview);
      renderTabs();
      await loadJobs();
    } catch (err) {
      toast(err.message);
    } finally {
      refreshing = undefined;
    }
  })();
  return refreshing;
}

$('job-queue').addEventListener('change', (e) => {
  state.queue = e.target.value;
  state.page = 0;
  localSet('queue', state.queue);
  renderTabs();
  void loadJobs();
});
$('prev').addEventListener('click', () => {
  state.page = Math.max(0, state.page - 1);
  void loadJobs();
});
$('next').addEventListener('click', () => {
  state.page += 1;
  void loadJobs();
});
$('retry-all').addEventListener('click', () => {
  if (state.queue) void action('Retrying all failed jobs', `queues/${q(state.queue)}/retry-failed`);
});
$('close-dialog').addEventListener('click', () => $('job-dialog').close());

void refresh();
setInterval(() => void refresh(), REFRESH_MS);
connectEvents();
