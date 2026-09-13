import { useState, useEffect, useMemo } from 'react';
import { API_BASE_URL } from '../config';
import { tealScale } from '../chartUtils';

const CELL = 11;
const GAP = 2;
const MIN_YEAR_EPISODES = 100; // skip sparse early years with barely any activity
const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const dateKey = (d) => {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
};

// Builds the GitHub-style week columns for one calendar year: starts on the
// Sunday on/before Jan 1 and ends on the Saturday on/after Dec 31.
const buildYearGrid = (year) => {
  const start = new Date(year, 0, 1);
  start.setDate(start.getDate() - start.getDay());
  const end = new Date(year, 11, 31);
  end.setDate(end.getDate() + (6 - end.getDay()));

  const weeks = [];
  let cursor = new Date(start);
  while (cursor <= end) {
    const week = [];
    for (let d = 0; d < 7; d++) {
      week.push(cursor.getFullYear() === year ? new Date(cursor) : null);
      cursor.setDate(cursor.getDate() + 1);
    }
    weeks.push(week);
  }
  return weeks;
};

export default function EpisodeCalendarHeatmap() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [hover, setHover] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/stats/episode-calendar`)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  const { years, countsByDate, maxCount } = useMemo(() => {
    if (!data) return { years: [], countsByDate: {}, maxCount: 1 };
    const countsByDate = {};
    const perYear = {};
    data.days.forEach(d => {
      countsByDate[d.published_date] = d.count;
      const year = parseInt(d.published_date.slice(0, 4), 10);
      perYear[year] = (perYear[year] || 0) + d.count;
    });
    const years = Object.keys(perYear)
      .map(Number)
      .filter(y => perYear[y] >= MIN_YEAR_EPISODES)
      .sort((a, b) => a - b);
    const maxCount = Math.max(...data.days.map(d => d.count), 1);
    return { years, countsByDate, maxCount };
  }, [data]);

  if (error) return <p className="text-sm text-red-500">Couldn't load this chart: {error}</p>;
  if (!data) return <p className="text-sm text-gray-400">Loading...</p>;

  return (
    <div className="overflow-x-auto">
      <div className="inline-block">
        {years.map(year => {
          const weeks = buildYearGrid(year);
          // Which week-columns start a new month, for labels along the top
          const monthLabels = [];
          let lastMonth = -1;
          weeks.forEach((week, wi) => {
            const firstRealDay = week.find(d => d !== null);
            if (firstRealDay && firstRealDay.getMonth() !== lastMonth) {
              lastMonth = firstRealDay.getMonth();
              monthLabels.push({ wi, label: MONTH_NAMES[lastMonth] });
            }
          });

          return (
            <div key={year} className="flex items-start gap-2 mb-3">
              <div className="text-xs text-gray-500 w-10 pt-4 flex-shrink-0">{year}</div>
              <div>
                <div style={{ position: 'relative', height: 12, width: weeks.length * (CELL + GAP) }}>
                  {monthLabels.map(({ wi, label }) => (
                    <span key={wi} style={{ position: 'absolute', left: wi * (CELL + GAP), fontSize: 10, color: '#9ca3af' }}>
                      {label}
                    </span>
                  ))}
                </div>
                <div style={{ display: 'flex', gap: GAP }}>
                  {weeks.map((week, wi) => (
                    <div key={wi} style={{ display: 'flex', flexDirection: 'column', gap: GAP }}>
                      {week.map((day, di) => {
                        if (!day) return <div key={di} style={{ width: CELL, height: CELL }} />;
                        const key = dateKey(day);
                        const count = countsByDate[key] || 0;
                        const isHovered = hover?.key === key;
                        return (
                          <div
                            key={di}
                            onMouseEnter={() => setHover({ key, count, date: day })}
                            onMouseLeave={() => setHover(null)}
                            title={`${key}: ${count} episode${count !== 1 ? 's' : ''}`}
                            style={{
                              width: CELL, height: CELL,
                              background: count === 0 ? '#f3f4f6' : tealScale(count / maxCount),
                              border: isHovered ? '1.5px solid #0d9488' : 'none',
                              borderRadius: 2,
                              cursor: 'pointer',
                            }}
                          />
                        );
                      })}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      <div className="flex items-center gap-2 mt-2 text-xs text-gray-500">
        <span>Fewer episodes</span>
        <div className="flex">
          <div style={{ width: 14, height: 12, background: '#f3f4f6' }} />
          {[0.2, 0.4, 0.6, 0.8, 1].map(t => (
            <div key={t} style={{ width: 14, height: 12, background: tealScale(t) }} />
          ))}
        </div>
        <span>More episodes (max {maxCount}/day)</span>
      </div>
    </div>
  );
}
