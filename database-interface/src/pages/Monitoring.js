import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  Database, 
  Users, 
  TrendingUp,
  Calendar,
  BarChart3,
  FileText,
  Clock,
  Target,
  Shield,
  RefreshCw,
  AlertCircle
} from 'lucide-react';
import { databaseAPI } from '../services/api';

const Monitoring = () => {
  const [overview, setOverview] = useState(null);
  const [campaignDistribution, setCampaignDistribution] = useState(null);
  const [recentActivity, setRecentActivity] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(new Date());

  useEffect(() => {
    loadMonitoringData();
  }, []);

  const loadMonitoringData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      // Load all monitoring data in parallel
      const [overviewResponse, distributionResponse, activityResponse] = await Promise.all([
        databaseAPI.getMonitoringOverview(),
        databaseAPI.getMonitoringCampaignDistribution(),
        databaseAPI.getMonitoringRecentActivity(100)
      ]);

      setOverview(overviewResponse.data.overview);
      setCampaignDistribution(distributionResponse.data.campaign_distribution);
      setRecentActivity(activityResponse.data.recent_activity);
      setLastUpdated(new Date());
      
    } catch (err) {
      console.error('Error loading monitoring data:', err);
      setError('Ошибка загрузки данных мониторинга. Проверьте соединение с сервером.');
    } finally {
      setIsLoading(false);
    }
  };

  const formatNumber = (num) => {
    if (num === null || num === undefined) return 'Н/Д';
    return num.toLocaleString();
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'Н/Д';
    try {
      return new Date(dateStr).toLocaleDateString('ru-RU');
    } catch {
      return dateStr;
    }
  };

  const formatDateTime = (dateStr) => {
    if (!dateStr) return 'Н/Д';
    try {
      return new Date(dateStr).toLocaleString('ru-RU');
    } catch {
      return dateStr;
    }
  };

  const getStatusBadge = (status) => {
    switch (status) {
      case 'Active':
        return <span className="badge badge-success">Активная</span>;
      case 'Inactive':
        return <span className="badge badge-secondary">Неактивная</span>;
      default:
        return <span className="badge badge-secondary">{status}</span>;
    }
  };

  const getActivityTypeIcon = (activityType) => {
    switch (activityType) {
      case 'Control Group':
        return <Shield size={16} className="text-blue-500" />;
      case 'Target Group':
        return <Target size={16} className="text-green-500" />;
      case 'SPSS Target':
        return <Database size={16} className="text-purple-500" />;
      default:
        return <Activity size={16} className="text-gray-500" />;
    }
  };

  if (isLoading) {
    return (
      <div className="monitoring">
        <div className="dashboard-header">
          <h1 className="dashboard-title">Мониторинг системы</h1>
          <p className="dashboard-subtitle">Загрузка данных мониторинга...</p>
        </div>
        <div style={{ textAlign: 'center', padding: '3rem' }}>
          <div className="loading-spinner"></div>
          <p>Загрузка статистики...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="monitoring">
      <div className="dashboard-header">
        <div>
          <h1 className="dashboard-title">Мониторинг системы</h1>
          <p className="dashboard-subtitle">
            Статистика загрузок и распределения пользователей по кампаниям
          </p>
          <small style={{ color: '#6b7280' }}>
            Последнее обновление: {lastUpdated.toLocaleString('ru-RU')}
          </small>
        </div>
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
          <button className="btn btn-primary" onClick={loadMonitoringData}>
            <RefreshCw size={16} />
            Обновить
          </button>
        </div>
      </div>

      {error && (
        <div className="alert alert-error" style={{ marginBottom: '1.5rem' }}>
          <AlertCircle size={20} />
          {error}
        </div>
      )}

      {/* Overview Statistics */}
      {overview && (
        <div className="card" style={{ marginBottom: '1.5rem' }}>
          <div className="card-header">
            <h2 className="card-title">Обзор таблиц</h2>
          </div>
          <div className="stats-grid">
            <div className="stat-card">
              <div className="stat-icon">
                <Shield className="nav-icon" style={{ color: '#3b82f6' }} />
              </div>
              <div className="stat-content">
                <div className="stat-number">
                  {formatNumber(overview.tables.sc_local_control?.total_users)}
                </div>
                <div className="stat-label">Контрольная группа</div>
                <div className="stat-sublabel">
                  {formatNumber(overview.tables.sc_local_control?.unique_campaigns)} кампаний
                </div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <Target className="nav-icon" style={{ color: '#10b981' }} />
              </div>
              <div className="stat-content">
                <div className="stat-number">
                  {formatNumber(overview.tables.sc_local_target?.total_users)}
                </div>
                <div className="stat-label">Целевые группы</div>
                <div className="stat-sublabel">
                  {formatNumber(overview.tables.sc_local_target?.unique_campaigns)} кампаний
                </div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <Database className="nav-icon" style={{ color: '#8b5cf6' }} />
              </div>
              <div className="stat-content">
                <div className="stat-number">
                  {overview.tables.spss_sc_theory_users?.error 
                    ? 'Ошибка' 
                    : formatNumber(overview.tables.spss_sc_theory_users?.total_users)
                  }
                </div>
                <div className="stat-label">SPSS пользователи</div>
                <div className="stat-sublabel">
                  {overview.tables.spss_sc_theory_users?.error 
                    ? 'Нет подключения' 
                    : `${formatNumber(overview.tables.spss_sc_theory_users?.unique_campaigns)} кампаний`
                  }
                </div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <FileText className="nav-icon" style={{ color: '#f59e0b' }} />
              </div>
              <div className="stat-content">
                <div className="stat-number">
                  {formatNumber(overview.campaigns?.total_campaigns)}
                </div>
                <div className="stat-label">Всего кампаний</div>
                <div className="stat-sublabel">
                  {formatNumber(overview.campaigns?.active_campaigns)} активных
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
        {/* Campaign Distribution */}
        {campaignDistribution && (
          <div className="card">
            <div className="card-header">
              <h2 className="card-title">
                <TrendingUp size={20} />
                Распределение по кампаниям
              </h2>
              <div style={{ fontSize: '0.875rem', color: '#6b7280' }}>
                Всего: {formatNumber(campaignDistribution.totals?.total_actual_users)} пользователей
              </div>
            </div>
            <div className="table-container" style={{ maxHeight: '500px' }}>
              <table className="table">
                <thead>
                  <tr>
                    <th>Кампания</th>
                    <th>Статус</th>
                    <th>Контроль</th>
                    <th>Целевые</th>
                    <th>SPSS</th>
                    <th>Всего</th>
                  </tr>
                </thead>
                <tbody>
                  {campaignDistribution.campaigns?.slice(0, 20).map((campaign) => (
                    <tr key={campaign.theory_id}>
                      <td>
                        <div title={campaign.theory_name}>
                          <strong>{campaign.theory_id}</strong>
                          <div style={{ fontSize: '0.75rem', color: '#6b7280' }}>
                            {campaign.theory_name?.length > 30 
                              ? `${campaign.theory_name.substring(0, 30)}...`
                              : campaign.theory_name
                            }
                          </div>
                        </div>
                      </td>
                      <td>{getStatusBadge(campaign.status)}</td>
                      <td style={{ color: '#3b82f6' }}>{formatNumber(campaign.control_users)}</td>
                      <td style={{ color: '#10b981' }}>{formatNumber(campaign.target_users)}</td>
                      <td style={{ color: '#8b5cf6' }}>
                        {typeof campaign.spss_users === 'string' && campaign.spss_users.includes('Error') 
                          ? 'Ошибка' 
                          : formatNumber(campaign.spss_users)
                        }
                      </td>
                      <td style={{ fontWeight: 'bold' }}>{formatNumber(campaign.total_actual_users)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Recent Activity */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">
              <Activity size={20} />
              Последняя активность
            </h2>
            <div style={{ fontSize: '0.875rem', color: '#6b7280' }}>
              {recentActivity?.summary?.period && (
                <span>{recentActivity.summary.period} • </span>
              )}
              {recentActivity?.summary?.total_users_uploaded ? (
                <span>{formatNumber(recentActivity.summary.total_users_uploaded)} пользователей</span>
              ) : (
                <span>Нет данных</span>
              )}
            </div>
          </div>
          <div className="table-container" style={{ maxHeight: '500px' }}>
            {recentActivity?.activities && recentActivity.activities.length > 0 ? (
              <table className="table">
                <thead>
                  <tr>
                    <th>Тип</th>
                    <th>Кампания</th>
                    <th>Пользователи</th>
                    <th>Время</th>
                    <th>Доп. поля</th>
                  </tr>
                </thead>
                <tbody>
                  {recentActivity.activities.map((activity, index) => (
                    <tr key={index}>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          {getActivityTypeIcon(activity.activity_type)}
                          <span style={{ fontSize: '0.75rem' }}>
                            {activity.activity_type}
                          </span>
                        </div>
                      </td>
                      <td>
                        <code style={{ fontSize: '0.75rem' }}>{activity.theory_id}</code>
                      </td>
                      <td>{formatNumber(activity.users_count)}</td>
                      <td style={{ fontSize: '0.75rem' }}>
                        {formatDateTime(activity.upload_time)}
                      </td>
                      <td style={{ fontSize: '0.75rem', color: '#6b7280' }}>
                        {activity.tab1 && (
                          <div title={`tab1: ${activity.tab1}, tab2: ${activity.tab2}`}>
                            {activity.tab1?.length > 15 
                              ? `${activity.tab1.substring(0, 15)}...`
                              : activity.tab1
                            }
                          </div>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
                <Activity size={48} style={{ margin: '0 auto 1rem', opacity: 0.3 }} />
                <p>Нет данных о последней активности</p>
                <p style={{ fontSize: '0.875rem' }}>
                  Возможно, в последние дни не было загрузок данных
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Monitoring; 