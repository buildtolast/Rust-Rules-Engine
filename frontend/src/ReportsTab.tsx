import React, { useState, useEffect } from 'react';
import { Download, Calendar, Filter, Info, AlertTriangle, CheckCircle } from 'lucide-react';
import type { AuditRecord } from './types';

interface ReportsTabProps {
  // Add props if needed
}

export const ReportsTab: React.FC<ReportsTabProps> = () => {
  const [fromDate, setFromDate] = useState<string>(
    new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 16)
  );
  const [toDate, setToDate] = useState<string>(
    new Date().toISOString().slice(0, 16)
  );
  const [topMatched, setTopMatched] = useState<AuditRecord[]>([]);
  const [topUnmatched, setTopUnmatched] = useState<AuditRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchTopRecords = async () => {
    setLoading(true);
    setError(null);
    try {
      const from = new Date(fromDate).toISOString();
      const to = new Date(toDate).toISOString();

      const [matchedRes, unmatchedRes] = await Promise.all([
        fetch(`/api/reports/top?type=MATCHED&from=${from}&to=${to}`),
        fetch(`/api/reports/top?type=UNMATCHED&from=${from}&to=${to}`)
      ]);

      if (!matchedRes.ok || !unmatchedRes.ok) throw new Error('Failed to fetch reports');

      const matched = await matchedRes.json();
      const unmatched = await unmatchedRes.json();

      setTopMatched(matched);
      setTopUnmatched(unmatched);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTopRecords();
  }, []);

  const handleExport = (type: 'MATCHED' | 'UNMATCHED') => {
    const from = new Date(fromDate).toISOString();
    const to = new Date(toDate).toISOString();
    window.open(`/api/reports/export?type=${type}&from=${from}&to=${to}`, '_blank');
  };

  const renderTable = (records: AuditRecord[], title: string, icon: React.ReactNode, type: 'matched' | 'unmatched') => (
    <div className="bg-white rounded-3xl shadow-sm border border-gray-100 overflow-hidden mb-8">
      <div className="px-6 py-5 border-b border-gray-50 flex items-center justify-between bg-gray-50/50">
        <div className="flex items-center gap-3">
          <div className={`p-2 rounded-xl ${type === 'matched' ? 'bg-green-100 text-green-600' : 'bg-amber-100 text-amber-600'}`}>
            {icon}
          </div>
          <h3 className="text-lg font-bold text-gray-900">{title}</h3>
        </div>
        <button
          onClick={() => handleExport(type.toUpperCase() as any)}
          className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 rounded-xl text-sm font-bold text-gray-700 hover:bg-gray-50 transition-colors shadow-sm"
        >
          <Download size={16} />
          Export All
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-gray-50/30">
              <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase tracking-wider">Rule ID</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase tracking-wider">Timestamp</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase tracking-wider">Reason / Expression</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase tracking-wider">Kafka Metadata</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {records.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-10 text-center text-gray-400 font-medium">
                  No records found for this period.
                </td>
              </tr>
            ) : (
              records.map((rec) => (
                <tr key={rec.auditId} className="hover:bg-gray-50/50 transition-colors group">
                  <td className="px-6 py-4">
                    <span className="font-mono text-sm font-bold text-indigo-600 bg-indigo-50 px-2 py-1 rounded-lg">
                      {rec.ruleId}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <div className="text-sm text-gray-900 font-medium">
                      {new Date(rec.timestamp).toLocaleDateString()}
                    </div>
                    <div className="text-xs text-gray-500">
                      {new Date(rec.timestamp).toLocaleTimeString()}
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <div className="max-w-md">
                      <p className="text-sm text-gray-700 line-clamp-2 italic">
                        {rec.reason || (type === 'matched' ? 'Rule matched successfully' : 'N/A')}
                      </p>
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <div className="text-xs font-mono text-gray-500 bg-gray-100 p-2 rounded-lg">
                      {rec.sourceTopic} [{rec.partition}:{rec.offset}]
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      {/* Filters */}
      <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100 flex flex-col md:flex-row items-end gap-6 mb-8">
        <div className="flex-1 w-full">
          <label className="block text-xs font-bold text-gray-400 uppercase tracking-widest mb-2 flex items-center gap-2">
            <Calendar size={14} /> From Date
          </label>
          <input
            type="datetime-local"
            value={fromDate}
            onChange={(e) => setFromDate(e.target.value)}
            className="w-full bg-gray-50 border border-gray-200 rounded-2xl px-4 py-3 text-gray-900 font-medium focus:ring-2 focus:ring-indigo-500 outline-none transition-all"
          />
        </div>
        <div className="flex-1 w-full">
          <label className="block text-xs font-bold text-gray-400 uppercase tracking-widest mb-2 flex items-center gap-2">
            <Calendar size={14} /> To Date
          </label>
          <input
            type="datetime-local"
            value={toDate}
            onChange={(e) => setToDate(e.target.value)}
            className="w-full bg-gray-50 border border-gray-200 rounded-2xl px-4 py-3 text-gray-900 font-medium focus:ring-2 focus:ring-indigo-500 outline-none transition-all"
          />
        </div>
        <button
          onClick={fetchTopRecords}
          disabled={loading}
          className="bg-indigo-600 text-white px-8 py-3.5 rounded-2xl font-bold shadow-lg shadow-indigo-100 hover:bg-indigo-700 disabled:opacity-50 transition-all flex items-center gap-2 min-w-[140px] justify-center"
        >
          {loading ? (
            <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
          ) : (
            <>
              <Filter size={18} /> Apply Filter
            </>
          )}
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-100 p-4 rounded-2xl flex items-center gap-3 text-red-600 font-medium">
          <AlertTriangle size={20} />
          {error}
        </div>
      )}

      {/* Top 10 Matched */}
      {renderTable(topMatched, "Top 10 Matched Events", <CheckCircle size={20} />, 'matched')}

      {/* Top 10 Unmatched */}
      {renderTable(topUnmatched, "Top 10 Unmatched Events", <Info size={20} />, 'unmatched')}
    </div>
  );
};
