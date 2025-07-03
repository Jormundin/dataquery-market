import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { 
  Database, 
  Search, 
  BarChart3, 
  Users, 
  Activity,
  TrendingUp,
  Clock,
  User
} from 'lucide-react';
import { databaseAPI, dataAPI } from '../services/api';

const Dashboard = () => {
  const [stats, setStats] = useState({
    totalQueries: 0,
    activeDatabases: 0,
    totalUsers: 0,
    avgResponseTime: '0s'
  });
  const [recentQueries, setRecentQueries] = useState([]);
  const [databases, setDatabases] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadDashboardData();
  }, []);

  const loadDashboardData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      // Load dashboard statistics from the backend
      const statsResponse = await databaseAPI.getStats();
      
      // Load databases - backend returns direct array
      const dbResponse = await databaseAPI.getDatabases();
      setDatabases(dbResponse.data || []); 
      
      // Load query history - backend returns direct array  
      const historyResponse = await databaseAPI.getQueryHistory(1, 4);
      setRecentQueries(historyResponse.data || []); 
      
      // Update stats from backend response
      setStats({
        totalQueries: statsResponse.data.total_queries || 0,
        activeDatabases: statsResponse.data.active_databases || 0,
        totalUsers: statsResponse.data.total_users || 0,
        avgResponseTime: statsResponse.data.avg_response_time || '0s'
      });
      
    } catch (err) {
      console.error('Error loading dashboard data:', err);
      setError('Ошибка загрузки данных. Проверьте соединение с сервером.');
      
      // Set empty data when API fails - no dummy data
      setStats({
        totalQueries: 0,
        activeDatabases: 0,
        totalUsers: 0,
        avgResponseTime: 'Н/Д'
      });
      
      setRecentQueries([]); // Empty array - no dummy data
      setDatabases([]); // Empty array - no dummy data
    } finally {
      setIsLoading(false);
    }
  };

  const testConnection = async () => {
    try {
      await databaseAPI.testConnection({});
      alert('Соединение успешно!');
    } catch (err) {
      alert('Ошибка соединения: ' + (err.response?.data?.detail || err.message));
    }
  };

  const getStatusBadge = (status) => {
    switch (status) {
      case 'success':
        return <span className="badge badge-success">Выполнено</span>;
      case 'pending':
        return <span className="badge badge-warning">В процессе</span>;
      case 'error':
        return <span className="badge badge-error">Ошибка</span>;
      case 'active':
        return <span className="badge badge-success">Активна</span>;
      case 'maintenance':
        return <span className="badge badge-warning">Обслуживание</span>;
      default:
        return <span className="badge badge-secondary">{status}</span>;
    }
  };

  const formatTimeAgo = (timestamp) => {
    const now = new Date();
    const queryTime = new Date(timestamp);
    const diffInMinutes = Math.floor((now - queryTime) / (1000 * 60));
    
    if (diffInMinutes < 1) {
      return 'Только что';
    } else if (diffInMinutes < 60) {
      return `${diffInMinutes} мин назад`;
    } else if (diffInMinutes < 1440) {
      const hours = Math.floor(diffInMinutes / 60);
      return `${hours} ч назад`;
    } else {
      const days = Math.floor(diffInMinutes / 1440);
      return `${days} д назад`;
    }
  };

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h1 className="dashboard-title">Панель управления</h1>
        <p className="dashboard-subtitle">
          Обзор активности базы данных и статистика запросов
        </p>
      </div>

      {error && (
        <div className="alert alert-error" style={{ marginBottom: '1.5rem' }}>
          {error}
        </div>
      )}

      {isLoading ? (
        <div style={{ textAlign: 'center', padding: '2rem' }}>
          <div className="loading-spinner"></div>
          <p>Загрузка данных...</p>
        </div>
      ) : (
        <>
          {/* Statistics Cards */}
          <div className="stats-grid">
            <div className="stat-card">
              <div className="stat-icon">
                <Search className="nav-icon" />
              </div>
              <div className="stat-content">
                <div className="stat-number">{stats.totalQueries.toLocaleString()}</div>
                <div className="stat-label">Всего запросов</div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <Database className="nav-icon" />
              </div>
              <div className="stat-content">
                <div className="stat-number">{stats.activeDatabases}</div>
                <div className="stat-label">Активных БД</div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <Users className="nav-icon" />
              </div>
              <div className="stat-content">
                <div className="stat-number">
                  {stats.totalUsers > 0 ? stats.totalUsers : 'Н/Д'}
                </div>
                <div className="stat-label">Пользователей</div>
              </div>
            </div>

            <div className="stat-card">
              <div className="stat-icon">
                <Clock className="nav-icon" />
              </div>
              <div className="stat-content">
                <div className="stat-number">{stats.avgResponseTime}</div>
                <div className="stat-label">Среднее время ответа</div>
              </div>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
            {/* Recent Queries */}
            <div className="card">
              <div className="card-header">
                <h2 className="card-title">Последние запросы</h2>
              </div>
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Запрос</th>
                      <th>Таблица</th>
                      <th>Пользователь</th>
                      <th>Время</th>
                      <th>Статус</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentQueries.length > 0 ? (
                      recentQueries.map((query) => (
                        <tr key={query.id}>
                          <td title={query.sql}>
                            {query.sql.length > 50 
                              ? `${query.sql.substring(0, 50)}...` 
                              : query.sql
                            }
                          </td>
                          <td><code>{query.table}</code></td>
                          <td>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                              <User size={14} />
                              {query.user || 'N/A'}
                            </div>
                          </td>
                          <td>{formatTimeAgo(query.created_at)}</td>
                          <td>{getStatusBadge(query.status)}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan="5" style={{ textAlign: 'center', color: '#6b7280', padding: '2rem' }}>
                          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.5rem' }}>
                            <Search size={32} style={{ opacity: 0.3 }} />
                            <span>Нет данных о запросах</span>
                            <small style={{ opacity: 0.7 }}>Выполните запрос для отображения истории</small>
                          </div>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Database Status */}
            <div className="card">
              <div className="card-header">
                <h2 className="card-title">Статус баз данных</h2>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                {databases.length > 0 ? (
                  databases.map((db, index) => (
                    <div key={index} className="database-status-item">
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                        <span style={{ fontWeight: '500' }}>{db.name}</span>
                        {getStatusBadge(db.status)}
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.875rem', color: '#6b7280' }}>
                        <span>{db.tables} таблиц</span>
                        <span>Доступ: {db.lastAccess}</span>
                      </div>
                    </div>
                  ))
                ) : (
                  <div style={{ textAlign: 'center', color: '#6b7280', padding: '2rem' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.5rem' }}>
                      <Database size={32} style={{ opacity: 0.3 }} />
                      <span>Нет доступных баз данных</span>
                      <small style={{ opacity: 0.7 }}>Проверьте подключение к серверу</small>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Quick Actions */}
          <div className="card">
            <div className="card-header">
              <h2 className="card-title">Быстрые действия</h2>
            </div>
            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
              <Link to="/query-builder" className="btn btn-primary">
                <Search className="nav-icon" style={{ width: '16px', height: '16px' }} />
                Создать новый запрос
              </Link>
              <Link to="/data-viewer" className="btn btn-secondary">
                <BarChart3 className="nav-icon" style={{ width: '16px', height: '16px' }} />
                Просмотр данных
              </Link>
              <button className="btn btn-success" onClick={testConnection}>
                <Database className="nav-icon" style={{ width: '16px', height: '16px' }} />
                Тест соединения
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default Dashboard; 