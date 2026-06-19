import React, { useState, useEffect } from 'react';
import { Send, Zap, Clock, AlertCircle, CheckCircle2, BarChart3, Activity } from 'lucide-react';
import type { AnalyticsStats } from './types';

const SimulationPanel: React.FC = () => {
  const [messageCount, setMessageCount] = useState<number>(100);
  const [isSimulating, setIsSimulating] = useState(false);
  const [stats, setStats] = useState<AnalyticsStats | null>(null);
  const [lastResult, setLastResult] = useState<{ count: number; message: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchStats = async () => {
    try {
      // 5-minute window so recent simulation runs are always visible
      const response = await fetch('/api/analytics/stats?from=' + new Date(Date.now() - 300000).toISOString());
      if (response.ok) {
        const data = await response.json();
        setStats(data);
      }
    } catch {
      // ignore — backend may be temporarily unavailable
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 15000); // poll every 15s
    return () => clearInterval(interval);
  }, []);

  const handleSimulate = async () => {
    setIsSimulating(true);
    setError(null);
    setLastResult(null);
    try {
      const response = await fetch(`/api/simulation/push?count=${messageCount}`, { method: 'POST' });
      if (response.ok) {
        const result = await response.json();
        setLastResult(result);
        // Pipeline takes a few seconds to process — refresh stats after a short delay
        setTimeout(fetchStats, 4000);
        setTimeout(fetchStats, 10000);
      } else {
        setError('Failed to trigger simulation');
      }
    } catch {
      setError('Connection error occurred');
    } finally {
      setIsSimulating(false);
    }
  };

  const formatNano = (nano: number) => {
    if (nano === 0) return '0ms';
    const ms = nano / 1_000_000;
    if (ms < 1) return ms.toFixed(3) + 'ms';
    return ms.toFixed(2) + 'ms';
  };

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      {/* Simulation Controls */}
      <div className="bg-white p-8 rounded-3xl shadow-sm border border-gray-100">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2 bg-indigo-50 text-indigo-600 rounded-xl">
            <Zap size={24} />
          </div>
          <h2 className="text-2xl font-bold text-gray-900">Traffic Simulator</h2>
        </div>
        
        <p className="text-gray-600 mb-8 max-w-2xl">
          Generate and push synthetic JSON events into the Kafka source topic to test your rules at scale. 
          Monitor the real-time processing latency and evaluation performance below.
        </p>

        <div className="flex flex-col sm:flex-row items-end gap-4 max-w-xl">
          <div className="flex-grow space-y-2">
            <label className="text-sm font-bold text-gray-500 uppercase tracking-wider ml-1">
              Number of Messages
            </label>
            <input
              type="number"
              value={messageCount}
              onChange={(e) => setMessageCount(parseInt(e.target.value) || 0)}
              className="w-full px-5 py-3.5 bg-gray-50 border border-gray-200 rounded-2xl focus:ring-4 focus:ring-indigo-50 focus:border-indigo-500 transition-all outline-none font-medium"
              placeholder="e.g. 1000"
              min="1"
              max="10000"
            />
          </div>
          <button
            onClick={handleSimulate}
            disabled={isSimulating}
            className={`flex items-center gap-2 px-8 py-4 rounded-2xl font-bold text-white shadow-lg transition-all ${
              isSimulating 
                ? 'bg-gray-400 cursor-not-allowed' 
                : 'bg-indigo-600 hover:bg-indigo-700 active:scale-95 shadow-indigo-200'
            }`}
          >
            {isSimulating ? (
              <>
                <div className="w-5 h-5 border-3 border-white/30 border-t-white rounded-full animate-spin" />
                Processing...
              </>
            ) : (
              <>
                <Send size={20} />
                Push Messages
              </>
            )}
          </button>
        </div>

        {error && (
          <div className="mt-6 p-4 bg-red-50 border border-red-100 rounded-2xl flex items-center gap-3 text-red-700 animate-in slide-in-from-top-2">
            <AlertCircle size={20} />
            <span className="font-medium">{error}</span>
          </div>
        )}

        {lastResult && (
          <div className="mt-6 p-4 bg-emerald-50 border border-emerald-100 rounded-2xl flex items-center gap-3 text-emerald-700 animate-in slide-in-from-top-2">
            <CheckCircle2 size={20} />
            <span className="font-medium">{lastResult.message}</span>
          </div>
        )}
      </div>

      {/* Real-time Latency Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-amber-50 rounded-2xl flex items-center justify-center text-amber-600">
              <Clock size={24} />
            </div>
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wider">Avg JSON Parsing</p>
              <h3 className="text-2xl font-black text-gray-900">
                {formatNano(stats?.avgParseTimeNano || 0)}
              </h3>
            </div>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
            <div className="h-full bg-amber-500 rounded-full" style={{ width: '40%' }}></div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-purple-50 rounded-2xl flex items-center justify-center text-purple-600">
              <Zap size={24} />
            </div>
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wider">Avg Rule Evaluation</p>
              <h3 className="text-2xl font-black text-gray-900">
                {formatNano(stats?.avgEvalTimeNano || 0)}
              </h3>
            </div>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
            <div className="h-full bg-purple-500 rounded-full" style={{ width: '60%' }}></div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-blue-50 rounded-2xl flex items-center justify-center text-blue-600">
              <BarChart3 size={24} />
            </div>
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wider">Total End-to-End</p>
              <h3 className="text-2xl font-black text-gray-900">
                {formatNano(stats?.avgTotalTimeNano || 0)}
              </h3>
            </div>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
            <div className="h-full bg-blue-500 rounded-full" style={{ width: '100%' }}></div>
          </div>
        </div>
      </div>

      {/* Real-time Status */}
      <div className="bg-gray-900 p-8 rounded-3xl shadow-xl text-white overflow-hidden relative">
        <div className="absolute top-0 right-0 p-8 opacity-10">
          <Activity size={160} />
        </div>
        <div className="relative z-10">
          <div className="flex items-center gap-2 mb-2 text-indigo-400">
            <div className="w-2 h-2 bg-indigo-400 rounded-full animate-ping"></div>
            <span className="text-xs font-bold uppercase tracking-widest">Live Processing Monitor</span>
          </div>
          <h3 className="text-3xl font-bold mb-6">Pipeline Activity (Last 5m)</h3>
          
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
            <div>
              <p className="text-gray-400 text-sm mb-1">Messages</p>
              <p className="text-4xl font-black">{stats?.totalMessages.toLocaleString() || 0}</p>
            </div>
            <div>
              <p className="text-gray-400 text-sm mb-1">Evaluations</p>
              <p className="text-4xl font-black">{stats?.totalEvaluations.toLocaleString() || 0}</p>
            </div>
            <div>
              <p className="text-gray-400 text-sm mb-1">Throughput</p>
              <p className="text-4xl font-black">{((stats?.totalMessages || 0) / 60).toFixed(1)} <span className="text-sm font-normal text-gray-500">msg/s</span></p>
            </div>
            <div>
              <p className="text-gray-400 text-sm mb-1">Health</p>
              <p className="text-4xl font-black text-emerald-400">100%</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SimulationPanel;
