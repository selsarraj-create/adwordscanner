import React, { useEffect, useState } from 'react';
import { supabase } from '../lib/supabaseClient';
import { useNavigate } from 'react-router-dom';
import {
    LogOut, RefreshCw, CheckCircle, XCircle, Clock,
    Search, Calendar, Filter, Download, Send
} from 'lucide-react';
import axios from 'axios';

const Admin = () => {
    const [leads, setLeads] = useState([]);
    const [loading, setLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [resendingId, setResendingId] = useState(null);
    const [selectedIds, setSelectedIds] = useState(new Set());
    const [bulkSending, setBulkSending] = useState(false);
    const [bulkProgress, setBulkProgress] = useState({ done: 0, total: 0 });
    const [webhookFilter, setWebhookFilter] = useState('all');
    const navigate = useNavigate();

    const API_URL = import.meta.env.MODE === 'production' ? '/api' : 'http://localhost:8000';

    useEffect(() => {
        const init = async () => {
            const { data: { session } } = await supabase.auth.getSession();
            if (!session) {
                navigate('/login');
            } else {
                fetchLeads();
            }
        };
        init();
    }, []);

    const checkAuth = async () => {
        const { data: { session } } = await supabase.auth.getSession();
        if (!session) {
            navigate('/login');
        }
    };

    const fetchLeads = async () => {
        setLoading(true);
        try {
            const { data, error } = await supabase
                .from('leads')
                .select('*')
                .order('created_at', { ascending: false });

            if (error) throw error;
            setLeads(data || []);
        } catch (error) {
            console.error('Error fetching leads:', error.message);
        } finally {
            setLoading(false);
        }
    };

    const handleLogout = async () => {
        await supabase.auth.signOut();
        navigate('/login');
    };

    const handleRetryWebhook = async (leadId) => {
        setResendingId(leadId);
        try {
            await axios.post(`${API_URL}/retry_webhook`, { lead_id: leadId });
            fetchLeads();
        } catch (error) {
            console.error('Retry failed:', error);
            alert('Failed to resend webhook. Check console for details.');
        } finally {
            setResendingId(null);
        }
    };

    const toggleSelect = (id) => {
        setSelectedIds(prev => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const toggleSelectAll = () => {
        if (selectedIds.size === filteredLeads.length) {
            setSelectedIds(new Set());
        } else {
            setSelectedIds(new Set(filteredLeads.map(l => l.id)));
        }
    };

    const handleBulkResend = async () => {
        const ids = Array.from(selectedIds);
        if (ids.length === 0) return;
        if (!confirm(`Resend CRM webhook for ${ids.length} lead(s)?`)) return;

        setBulkSending(true);
        setBulkProgress({ done: 0, total: ids.length });
        let failures = 0;
        const errors = [];

        for (let i = 0; i < ids.length; i++) {
            try {
                console.log(`[BULK RESEND] Sending ${i + 1}/${ids.length} — lead_id: ${ids[i]}`);
                const res = await axios.post(`${API_URL}/retry_webhook`, { lead_id: ids[i] });
                console.log(`[BULK RESEND] Response for ${ids[i]}:`, res.data);
            } catch (err) {
                failures++;
                const detail = err.response?.data?.error || err.response?.data?.detail || err.message;
                console.error(`[BULK RESEND] FAILED for ${ids[i]}:`, detail, err.response?.status);
                errors.push(`${ids[i]}: ${detail}`);
            }
            setBulkProgress({ done: i + 1, total: ids.length });
        }

        setBulkSending(false);
        setSelectedIds(new Set());
        fetchLeads();
        if (failures > 0) {
            alert(`${failures} of ${ids.length} resends failed.\n\n${errors.join('\n')}`);
        }
    };

    const filteredLeads = leads.filter(lead => {
        const matchesSearch =
            lead.first_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            lead.last_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            lead.email.toLowerCase().includes(searchTerm.toLowerCase());
        const matchesWebhook = webhookFilter === 'all' || lead.webhook_status === webhookFilter;
        return matchesSearch && matchesWebhook;
    });

    const getStatusIcon = (status) => {
        switch (status) {
            case 'success': return <CheckCircle size={16} className="text-green-400" />;
            case 'failed': return <XCircle size={16} className="text-red-400" />;
            default: return <Clock size={16} className="text-yellow-400" />;
        }
    };

    return (
        <div className="min-h-screen bg-pastel-bg text-pastel-text p-6">
            <div className="max-w-7xl mx-auto">
                {/* Header */}
                <div className="flex justify-between items-center mb-8">
                    <div>
                        <h1 className="text-2xl font-bold flex items-center gap-2">
                            <div className="w-8 h-8 rounded-full bg-pastel-accent flex items-center justify-center text-white text-sm">MS</div>
                            Model Scanner Admin
                        </h1>
                        <p className="text-pastel-muted text-sm mt-1">Manage leads and integrations</p>
                    </div>
                    <button
                        onClick={handleLogout}
                        className="flex items-center gap-2 px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 transition-colors text-sm"
                    >
                        <LogOut size={16} /> Sign Out
                    </button>
                </div>

                {/* Filters */}
                <div className="flex flex-col sm:flex-row gap-4 mb-6">
                    <div className="relative flex-1">
                        <Search className="absolute left-3 top-3 text-gray-500" size={18} />
                        <input
                            type="text"
                            placeholder="Search by name, email..."
                            className="w-full bg-white border border-gray-200 rounded-lg py-2.5 pl-10 pr-4 focus:outline-none focus:border-pastel-accent"
                            value={searchTerm}
                            onChange={e => setSearchTerm(e.target.value)}
                        />
                    </div>
                    <select
                        value={webhookFilter}
                        onChange={e => setWebhookFilter(e.target.value)}
                        className="bg-white border border-gray-200 rounded-lg py-2.5 px-4 text-sm focus:outline-none focus:border-pastel-accent cursor-pointer"
                    >
                        <option value="all">All Status</option>
                        <option value="success">✅ Success</option>
                        <option value="failed">❌ Failed</option>
                        <option value="pending">⏳ Pending</option>
                    </select>
                    <button onClick={fetchLeads} className="flex items-center gap-2 px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 transition-colors">
                        <RefreshCw size={18} className={loading ? "animate-spin" : ""} /> Refresh
                    </button>
                    {selectedIds.size > 0 && (
                        <button
                            onClick={handleBulkResend}
                            disabled={bulkSending}
                            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white font-semibold hover:bg-blue-500 transition-colors disabled:opacity-50"
                        >
                            <Send size={18} />
                            {bulkSending
                                ? `Sending ${bulkProgress.done}/${bulkProgress.total}...`
                                : `Resend CRM (${selectedIds.size})`
                            }
                        </button>
                    )}
                    <button className="flex items-center gap-2 px-4 py-2 rounded-lg bg-pastel-accent text-white font-semibold hover:bg-red-300 transition-colors">
                        <Download size={18} /> Export CSV
                    </button>
                </div>

                {/* Table */}
                <div className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm">
                    <div className="overflow-x-auto">
                        <table className="w-full text-left border-collapse">
                            <thead>
                                <tr className="bg-gray-50 border-b border-gray-100 text-xs uppercase text-pastel-muted">
                                    <th className="p-4 font-semibold">
                                        <input
                                            type="checkbox"
                                            checked={filteredLeads.length > 0 && selectedIds.size === filteredLeads.length}
                                            onChange={toggleSelectAll}
                                            className="w-4 h-4 rounded accent-pastel-accent cursor-pointer"
                                        />
                                    </th>
                                    <th className="p-4 font-semibold">Date</th>
                                    <th className="p-4 font-semibold">Photo</th>
                                    <th className="p-4 font-semibold">Name</th>
                                    <th className="p-4 font-semibold">Contact</th>
                                    <th className="p-4 font-semibold">Campaign</th>
                                    <th className="p-4 font-semibold">Score</th>
                                    <th className="p-4 font-semibold">Category</th>
                                    <th className="p-4 font-semibold text-center">Webhook</th>
                                    <th className="p-4 font-semibold text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-100 text-sm">
                                {loading ? (
                                    <tr><td colSpan="10" className="p-8 text-center text-gray-500">Loading leads...</td></tr>
                                ) : filteredLeads.length === 0 ? (
                                    <tr><td colSpan="10" className="p-8 text-center text-gray-500">No leads found matching your search.</td></tr>
                                ) : (
                                    filteredLeads.map(lead => (
                                        <tr key={lead.id} className={`hover:bg-gray-50 transition-colors ${selectedIds.has(lead.id) ? 'bg-blue-500/5' : ''}`}>
                                            <td className="p-4">
                                                <input
                                                    type="checkbox"
                                                    checked={selectedIds.has(lead.id)}
                                                    onChange={() => toggleSelect(lead.id)}
                                                    className="w-4 h-4 rounded accent-pastel-accent cursor-pointer"
                                                />
                                            </td>
                                            <td className="p-4 text-gray-400 whitespace-nowrap">
                                                {new Date(lead.created_at).toLocaleDateString()}
                                                <div className="text-xs opacity-60">{new Date(lead.created_at).toLocaleTimeString()}</div>
                                            </td>
                                            <td className="p-4">
                                                {lead.image_url ? (
                                                    <a href={lead.image_url} target="_blank" rel="noopener noreferrer" className="block w-10 h-10 rounded-full overflow-hidden border border-white/20 hover:border-pastel-accent transition-colors">
                                                        <img src={lead.image_url} alt="Lead" className="w-full h-full object-cover" />
                                                    </a>
                                                ) : (
                                                    <div className="w-10 h-10 rounded-full bg-white/5 flex items-center justify-center text-xs text-gray-500">No Img</div>
                                                )}
                                            </td>
                                            <td className="p-4 font-medium">
                                                {lead.first_name} {lead.last_name}
                                                <div className="text-xs text-gray-500">{lead.age} yo • {lead.city}</div>
                                            </td>
                                            <td className="p-4 text-gray-300">
                                                {lead.email}
                                                <div className="text-xs text-gray-500">{lead.phone}</div>
                                            </td>
                                            <td className="p-4">
                                                <span className="font-mono text-xs text-pastel-accent bg-pastel-accent/10 px-2 py-1 rounded border border-pastel-accent/20">
                                                    {lead.campaign || 'N/A'}
                                                </span>
                                            </td>
                                            <td className="p-4">
                                                <span className={`px-2 py-1 rounded text-xs font-bold ${lead.score >= 90 ? 'bg-green-500/20 text-green-400' :
                                                    lead.score >= 70 ? 'bg-yellow-500/20 text-yellow-400' :
                                                        'bg-red-500/20 text-red-400'
                                                    }`}>
                                                    {lead.score}
                                                </span>
                                            </td>
                                            <td className="p-4 text-gray-300 capitalize">{lead.category?.replace('_', ' ')}</td>
                                            <td className="p-4">
                                                <div className="flex justify-center">
                                                    <div
                                                        className={`flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium border ${lead.webhook_status === 'success' ? 'bg-green-500/10 border-green-500/20 text-green-400' :
                                                            lead.webhook_status === 'failed' ? 'bg-red-500/10 border-red-500/20 text-red-400' :
                                                                'bg-gray-500/10 border-gray-500/20 text-gray-400'
                                                            }`}
                                                        title={lead.webhook_response ? `Response: ${lead.webhook_response}` : ''}
                                                    >
                                                        {getStatusIcon(lead.webhook_status)}
                                                        <span className="capitalize">{lead.webhook_status}</span>
                                                    </div>
                                                </div>
                                            </td>
                                            <td className="p-4 text-right">
                                                {lead.webhook_status !== 'success' && (
                                                    <button
                                                        onClick={() => handleRetryWebhook(lead.id)}
                                                        disabled={resendingId === lead.id}
                                                        className="text-xs bg-blue-600 hover:bg-blue-500 text-white border border-blue-700 px-3 py-1.5 rounded font-medium transition-colors disabled:opacity-50"
                                                    >
                                                        {resendingId === lead.id ? 'Sending...' : 'Resend'}
                                                    </button>
                                                )}
                                            </td>
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>
                    <div className="p-4 border-t border-white/10 text-xs text-gray-500 flex justify-between">
                        <span>Showing {filteredLeads.length} leads</span>
                        <span>Auto-refreshing every 5 mins</span>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Admin;
