import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Plus, 
  Pencil, 
  Trash2, 
  CheckCircle, 
  XCircle, 
  Save, 
  X,
  RefreshCw,
  AlertCircle,
  Search,
  Database,
  Activity,
  Filter,
  TrendingUp,
  Settings,
  Zap,
  FileText,
  ShieldAlert,
  BarChart2,
  SlidersHorizontal,
  Cpu,
} from 'lucide-react';
import type { Rule } from './types';
import AnalyticsDashboard from './AnalyticsDashboard';
import SimulationPanel from './SimulationPanel';
import { ReportsTab } from './ReportsTab';
import { SreTab, useActiveOutages } from './SreTab';
import { MetricsTab } from './MetricsTab';
import { ConfigTab } from './ConfigTab';
import { TracingInsightsTab } from './TracingInsightsTab';
import { HealthBar } from './HealthBar';
import { AlertTriangle, X as XIcon } from 'lucide-react';

const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'management' | 'analytics' | 'simulator' | 'reports' | 'sre' | 'metrics' | 'config' | 'tracing'>('management');
  const [bannerDismissed, setBannerDismissed] = useState(false);
  const activeOutages = useActiveOutages();
  const [isInitializing, setIsInitializing] = useState(true);
  const [systemHealth, setSystemHealth] = useState<any>(null);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const response = await fetch('/health');
        if (response.ok) {
          const data = await response.json();
          setSystemHealth(data);
          if (data.status === 'ok' || data.status === 'HEALTHY') {
            setIsInitializing(false);
          }
        }
      } catch (error) {
        console.error('Health check failed', error);
      }
    };

    const interval = setInterval(checkHealth, 30000);
    checkHealth();

    return () => clearInterval(interval);
  }, []);
  const [rules, setRules] = useState<Rule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState<'all' | 'active' | 'inactive'>('all');
  const [currentRule, setCurrentRule] = useState<Partial<Rule>>({
    description: '',
    expression: '',
    enabled: true
  });

  const fetchRules = async () => {
    setLoading(true);
    try {
      const response = await axios.get<Rule[]>('/api/rules');
      setRules(response.data);
      setError(null);
    } catch (err) {
      setError('Failed to fetch rules. Make sure the backend is running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const loadRules = async () => {
      await fetchRules();
    };
    loadRules();
  }, []);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      if (currentRule.id) {
        await axios.put(`/api/rules/${currentRule.id}`, currentRule);
      } else {
        await axios.post('/api/rules', currentRule);
      }
      setIsModalOpen(false);
      setCurrentRule({ description: '', expression: '', enabled: true });
      fetchRules();
    } catch (err) {
      setError('Failed to save rule');
      console.error(err);
    }
  };

  const handleDelete = async (id: string) => {
    if (window.confirm('Are you sure you want to delete this rule?')) {
      try {
        await axios.delete(`/api/rules/${id}`);
        fetchRules();
      } catch (err) {
        setError('Failed to delete rule');
        console.error(err);
      }
    }
  };

  const openEditModal = (rule: Rule) => {
    setCurrentRule(rule);
    setIsModalOpen(true);
  };

  const openCreateModal = () => {
    setCurrentRule({ description: '', expression: '', enabled: true });
    setIsModalOpen(true);
  };

  const stats = {
    total: rules.length,
    active: rules.filter(r => r.enabled).length,
    inactive: rules.filter(r => !r.enabled).length
  };

  const filteredRules = rules.filter(rule => {
    const matchesSearch = rule.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         rule.expression.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesFilter = filterStatus === 'all' ? true :
                         filterStatus === 'active' ? rule.enabled : !rule.enabled;
    return matchesSearch && matchesFilter;
  });

  const showBanner = activeOutages.length > 0 && !bannerDismissed;

  React.useEffect(() => {
    if (activeOutages.length === 0) setBannerDismissed(false);
  }, [activeOutages.length]);

  if (isInitializing) {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center p-6 font-['Plus_Jakarta_Sans']">
        <div className="w-full max-w-md bg-white rounded-3xl shadow-xl border border-gray-100 p-10 text-center">
          <div className="flex justify-center mb-8">
            <div className="w-20 h-20 bg-indigo-50 rounded-2xl flex items-center justify-center text-indigo-600 animate-pulse">
              <Activity size={48} />
            </div>
          </div>
          <h2 className="text-2xl font-black text-gray-900 mb-4">Warming Up System</h2>
          <p className="text-gray-500 mb-8 leading-relaxed">
            Please wait while we initialize the Rules Engine, Kafka Streams, and database connections.
          </p>
          
          <div className="space-y-4 mb-8">
            {systemHealth?.components && Object.entries(systemHealth.components).map(([name, state]: [string, any]) => (
              <div key={name} className="flex items-center justify-between p-4 bg-gray-50 rounded-2xl border border-gray-100">
                <span className="text-sm font-bold text-gray-700 capitalize">
                  {name.replace(/([A-Z])/g, ' $1').trim()}
                </span>
                <span className={`px-3 py-1 rounded-full text-xs font-black uppercase tracking-tighter ${
                  state === 'RUNNING' || state === 'CONNECTED' || state === 'HEALTHY'
                    ? 'bg-green-100 text-green-700' 
                    : 'bg-amber-100 text-amber-700 animate-pulse'
                }`}>
                  {state}
                </span>
              </div>
            ))}
          </div>

          <div className="flex items-center justify-center gap-2 text-indigo-600 font-bold">
            <div className="w-2 h-2 bg-indigo-600 rounded-full animate-bounce" style={{ animationDelay: '0ms' }}></div>
            <div className="w-2 h-2 bg-indigo-600 rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
            <div className="w-2 h-2 bg-indigo-600 rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      {showBanner && (
        <div className="sticky top-0 z-50 bg-red-600 text-white px-4 py-3 flex items-center gap-3 shadow-lg">
          <AlertTriangle size={18} className="flex-shrink-0 animate-pulse" />
          <div className="flex-1 min-w-0">
            <span className="font-bold">Service Outage Detected — </span>
            <span className="text-red-100">
              {activeOutages.map(o => o.container.replace('rre-', '')).join(', ')}
              {activeOutages.length === 1 ? ' is' : ' are'} currently down.
            </span>
          </div>
          <button
            onClick={() => setActiveTab('sre')}
            className="flex-shrink-0 text-xs font-bold bg-white/20 hover:bg-white/30 px-3 py-1.5 rounded-lg transition-colors"
          >
            View Incidents
          </button>
          <button
            onClick={() => setBannerDismissed(true)}
            className="flex-shrink-0 hover:bg-white/20 p-1 rounded transition-colors"
            aria-label="Dismiss"
          >
            <XIcon size={16} />
          </button>
        </div>
      )}
      <HealthBar />
      <div className="max-w-6xl mx-auto p-4 md:p-8">
        <header className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 mb-8">
          <div>
            <h1 className="text-4xl font-extrabold tracking-tight text-gray-900 sm:text-5xl flex items-center gap-4">
              <span className="bg-indigo-600 text-white p-2 rounded-2xl shadow-lg shadow-indigo-200">
                <Activity size={32} strokeWidth={2.5} />
              </span>
              Rules Engine Dashboard
            </h1>
            <p className="mt-2 text-lg text-gray-600">
              Rules-based event streaming engine — evaluate, match, and route audit events in real time.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button 
              onClick={fetchRules}
              className="p-2.5 text-gray-500 hover:text-indigo-600 hover:bg-white hover:shadow-sm border border-transparent hover:border-gray-200 rounded-xl transition-all"
              title="Refresh rules"
            >
              <RefreshCw size={22} className={loading ? 'animate-spin' : ''} />
            </button>
            <button 
              onClick={openCreateModal}
              className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-5 py-2.5 rounded-xl font-semibold transition-all shadow-md shadow-indigo-200"
            >
              <Plus size={22} />
              <span>Add New Rule</span>
            </button>
          </div>
        </header>

        {/* Tab Switcher */}
        <div className="flex overflow-x-auto mb-8 border-b border-gray-200 scrollbar-none">
          {([
            { id: 'management', label: 'Rules',       icon: <Settings size={16} /> },
            { id: 'analytics',  label: 'Analytics',   icon: <TrendingUp size={16} /> },
            { id: 'simulator',  label: 'Simulator',   icon: <Zap size={16} /> },
            { id: 'reports',    label: 'Compliance',  icon: <FileText size={16} /> },
            { id: 'sre',        label: 'SRE',         icon: <ShieldAlert size={16} /> },
            { id: 'metrics',    label: 'Metrics',     icon: <BarChart2 size={16} /> },
            { id: 'config',     label: 'Config',      icon: <SlidersHorizontal size={16} /> },
            { id: 'tracing',    label: 'Tracing',     icon: <Cpu size={16} /> },
          ] as const).map(({ id, label, icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`flex flex-col items-center gap-1 px-4 py-3 text-xs font-bold whitespace-nowrap transition-all border-b-2 flex-shrink-0 ${
                activeTab === id
                  ? 'border-indigo-600 text-indigo-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {icon}
              {label}
            </button>
          ))}
        </div>

        {activeTab === 'management' ? (
          <>
            {/* Stats Section */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-6 mb-10">
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100 flex items-center gap-5 transition-all hover:shadow-md">
                <div className="w-14 h-14 bg-indigo-50 rounded-2xl flex items-center justify-center text-indigo-600 shadow-inner">
                  <Database size={28} />
                </div>
                <div>
                  <p className="text-sm font-bold text-gray-500 uppercase tracking-wider">Total Rules</p>
                  <h3 className="text-3xl font-black text-gray-900">{stats.total}</h3>
                </div>
              </div>
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100 flex items-center gap-5 transition-all hover:shadow-md">
                <div className="w-14 h-14 bg-emerald-50 rounded-2xl flex items-center justify-center text-emerald-600 shadow-inner">
                  <CheckCircle size={28} />
                </div>
                <div>
                  <p className="text-sm font-bold text-gray-500 uppercase tracking-wider">Active</p>
                  <h3 className="text-3xl font-black text-gray-900">{stats.active}</h3>
                </div>
              </div>
              <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100 flex items-center gap-5 transition-all hover:shadow-md">
                <div className="w-14 h-14 bg-orange-50 rounded-2xl flex items-center justify-center text-orange-600 shadow-inner">
                  <XCircle size={28} />
                </div>
                <div>
                  <p className="text-sm font-bold text-gray-500 uppercase tracking-wider">Inactive</p>
                  <h3 className="text-3xl font-black text-gray-900">{stats.inactive}</h3>
                </div>
              </div>
            </div>

            {/* Search & Filter Bar */}
            <div className="flex flex-col md:flex-row gap-4 mb-6">
              <div className="relative flex-grow">
                <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-400" size={20} />
                <input 
                  type="text"
                  placeholder="Search by description or expression..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full pl-12 pr-4 py-3.5 bg-white border border-gray-200 rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-500 outline-none transition-all shadow-sm"
                />
              </div>
              <div className="flex gap-2">
                <div className="bg-white p-1 rounded-2xl border border-gray-200 flex shadow-sm">
                  <button 
                    onClick={() => setFilterStatus('all')}
                    className={`px-4 py-2.5 rounded-xl text-sm font-bold transition-all ${filterStatus === 'all' ? 'bg-gray-900 text-white shadow-md' : 'text-gray-500 hover:bg-gray-50'}`}
                  >
                    All
                  </button>
                  <button 
                    onClick={() => setFilterStatus('active')}
                    className={`px-4 py-2.5 rounded-xl text-sm font-bold transition-all ${filterStatus === 'active' ? 'bg-gray-900 text-white shadow-md' : 'text-gray-500 hover:bg-gray-50'}`}
                  >
                    Active
                  </button>
                  <button 
                    onClick={() => setFilterStatus('inactive')}
                    className={`px-4 py-2.5 rounded-xl text-sm font-bold transition-all ${filterStatus === 'inactive' ? 'bg-gray-900 text-white shadow-md' : 'text-gray-500 hover:bg-gray-50'}`}
                  >
                    Inactive
                  </button>
                </div>
              </div>
            </div>

            {error && (
              <div className="mb-8 bg-red-50 border border-red-200 rounded-2xl p-4 flex items-start gap-3">
                <AlertCircle className="text-red-500 mt-0.5 flex-shrink-0" size={20} />
                <div>
                  <h4 className="text-red-800 font-bold">Error</h4>
                  <p className="text-red-700">{error}</p>
                </div>
                <button onClick={() => setError(null)} className="ml-auto text-red-400 hover:text-red-600">
                  <X size={20} />
                </button>
              </div>
            )}

            <div className="bg-white rounded-3xl shadow-xl shadow-gray-200/50 border border-gray-100 overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr className="bg-gray-50/50 border-b border-gray-100">
                      <th className="px-8 py-5 text-sm font-bold text-gray-500 uppercase tracking-wider">Status</th>
                      <th className="px-8 py-5 text-sm font-bold text-gray-500 uppercase tracking-wider">Description</th>
                      <th className="px-8 py-5 text-sm font-bold text-gray-500 uppercase tracking-wider">Rule Expression</th>
                      <th className="px-8 py-5 text-sm font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">Last Updated</th>
                      <th className="px-8 py-5 text-sm font-bold text-gray-500 uppercase tracking-wider text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {loading && rules.length === 0 ? (
                      <tr>
                        <td colSpan={5} className="px-8 py-24 text-center">
                          <div className="flex flex-col items-center gap-4">
                            <div className="relative">
                              <div className="w-12 h-12 border-4 border-indigo-100 border-t-indigo-600 rounded-full animate-spin"></div>
                            </div>
                            <span className="text-lg font-medium text-gray-500">Loading your rules...</span>
                          </div>
                        </td>
                      </tr>
                    ) : filteredRules.length === 0 ? (
                      <tr>
                        <td colSpan={5} className="px-8 py-24 text-center">
                          <div className="flex flex-col items-center gap-3">
                            <div className="w-16 h-16 bg-gray-50 rounded-full flex items-center justify-center text-gray-300 mb-2">
                              {searchTerm || filterStatus !== 'all' ? <Filter size={32} /> : <AlertCircle size={32} />}
                            </div>
                            <h3 className="text-xl font-bold text-gray-900">
                              {searchTerm || filterStatus !== 'all' ? 'No matching rules' : 'No rules found'}
                            </h3>
                            <p className="text-gray-500 max-w-xs mx-auto">
                              {searchTerm || filterStatus !== 'all' 
                                ? "Try adjusting your search or filters to find what you're looking for." 
                                : "Get started by creating your first audit rule using the button above."}
                            </p>
                            {(searchTerm || filterStatus !== 'all') && (
                              <button 
                                onClick={() => { setSearchTerm(''); setFilterStatus('all'); }}
                                className="mt-2 text-indigo-600 font-bold hover:underline"
                              >
                                Clear all filters
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ) : (
                      filteredRules.map((rule) => (
                        <tr key={rule.id} className="group hover:bg-gray-50/80 transition-all duration-200">
                          <td className="px-8 py-6">
                            {rule.enabled ? (
                              <span className="inline-flex items-center gap-1.5 text-emerald-700 bg-emerald-50 border border-emerald-100 px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wide">
                                <CheckCircle size={14} /> Active
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1.5 text-gray-500 bg-gray-50 border border-gray-200 px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wide">
                                <XCircle size={14} /> Inactive
                              </span>
                            )}
                          </td>
                          <td className="px-8 py-6">
                            <div className="font-semibold text-gray-900">{rule.description}</div>
                            <div className="text-xs text-gray-400 mt-0.5 font-mono">ID: {rule.id}</div>
                          </td>
                          <td className="px-8 py-6">
                            <div className="relative group/code">
                              <code className="block bg-gray-900 text-pink-400 px-4 py-2.5 rounded-xl text-sm font-mono break-all border border-gray-800 shadow-inner">
                                {rule.expression}
                              </code>
                            </div>
                          </td>
                          <td className="px-8 py-6 text-sm text-gray-500 whitespace-nowrap">
                            {rule.updatedAt ? (
                              <div className="flex flex-col">
                                <span className="font-medium text-gray-700">{new Date(rule.updatedAt).toLocaleDateString()}</span>
                                <span className="text-xs">{new Date(rule.updatedAt).toLocaleTimeString()}</span>
                              </div>
                            ) : 'N/A'}
                          </td>
                          <td className="px-8 py-6 text-right">
                            <div className="flex justify-end gap-3 opacity-0 group-hover:opacity-100 transition-opacity">
                              <button 
                                onClick={() => openEditModal(rule)}
                                className="p-2.5 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all"
                                title="Edit rule"
                              >
                                <Pencil size={20} />
                              </button>
                              <button 
                                onClick={() => handleDelete(rule.id!)}
                                className="p-2.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-xl transition-all"
                                title="Delete rule"
                              >
                                <Trash2 size={20} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : activeTab === 'analytics' ? (
          <AnalyticsDashboard rules={rules} />
        ) : activeTab === 'simulator' ? (
          <SimulationPanel />
        ) : activeTab === 'reports' ? (
          <ReportsTab />
        ) : activeTab === 'sre' ? (
          <SreTab />
        ) : activeTab === 'metrics' ? (
          <MetricsTab />
        ) : activeTab === 'tracing' ? (
          <TracingInsightsTab />
        ) : (
          <ConfigTab />
        )}
      </div>

      {isModalOpen && (
        <div className="fixed inset-0 flex items-center justify-center p-4 z-50">
          <div className="absolute inset-0 bg-gray-900/60 backdrop-blur-sm" onClick={() => setIsModalOpen(false)}></div>
          <div className="bg-white rounded-[2rem] shadow-2xl w-full max-w-xl overflow-hidden relative z-10">
            <div className="px-8 py-6 border-b border-gray-50 flex justify-between items-center">
              <div>
                <h3 className="text-2xl font-bold text-gray-900">
                  {currentRule.id ? 'Edit Rule' : 'Create Rule'}
                </h3>
                <p className="text-gray-500 text-sm">Enter the details for your audit rule.</p>
              </div>
              <button 
                onClick={() => setIsModalOpen(false)}
                className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-full transition-colors"
              >
                <X size={28} />
              </button>
            </div>
            <form onSubmit={handleSave}>
              <div className="p-8 space-y-6">
                <div className="space-y-2">
                  <label htmlFor="description" className="text-sm font-bold text-gray-700 ml-1">
                    Description
                  </label>
                  <input
                    id="description"
                    type="text"
                    required
                    value={currentRule.description}
                    onChange={(e) => setCurrentRule({ ...currentRule, description: e.target.value })}
                    className="w-full px-5 py-3.5 bg-gray-50 border border-gray-200 rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-500 outline-none transition-all placeholder:text-gray-400"
                    placeholder="Briefly describe the purpose of this rule"
                  />
                </div>
                <div className="space-y-2">
                  <label htmlFor="expression" className="text-sm font-bold text-gray-700 ml-1">
                    Rule Expression
                  </label>
                  <textarea
                    id="expression"
                    required
                    rows={4}
                    value={currentRule.expression}
                    onChange={(e) => setCurrentRule({ ...currentRule, expression: e.target.value })}
                    className="w-full px-5 py-3.5 bg-gray-900 text-pink-400 border border-gray-800 rounded-2xl focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-500 outline-none transition-all font-mono text-sm placeholder:text-gray-600 shadow-inner"
                    placeholder="e.g. payload.amount > 1000 && payload.currency == 'USD'"
                  />
                </div>
                <div className="flex items-center gap-3 p-4 bg-gray-50 rounded-2xl border border-gray-100 transition-colors hover:border-gray-200">
                  <div className="relative inline-flex items-center cursor-pointer">
                    <input
                      id="active"
                      type="checkbox"
                      checked={currentRule.enabled}
                      onChange={(e) => setCurrentRule({ ...currentRule, enabled: e.target.checked })}
                      className="sr-only peer"
                    />
                    <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-indigo-600"></div>
                  </div>
                  <label htmlFor="active" className="text-sm font-bold text-gray-700 cursor-pointer">
                    Active Rule
                  </label>
                </div>
              </div>
              <div className="px-8 py-6 bg-gray-50/50 border-t border-gray-50 flex justify-end gap-4">
                <button
                  type="button"
                  onClick={() => setIsModalOpen(false)}
                  className="px-6 py-3 font-bold text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-xl transition-all"
                >
                  Discard
                </button>
                <button
                  type="submit"
                  className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-8 py-3 rounded-xl font-bold transition-all shadow-lg shadow-indigo-200"
                >
                  <Save size={20} />
                  <span>{currentRule.id ? 'Update Rule' : 'Save Rule'}</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;
