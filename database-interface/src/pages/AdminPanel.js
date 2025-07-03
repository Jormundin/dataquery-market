import React, { useState, useEffect } from 'react';
import { 
  Shield, 
  Mail, 
  TestTube, 
  Eye, 
  Calendar,
  AlertCircle,
  CheckCircle,
  Send,
  Clock,
  Database
} from 'lucide-react';
import api from '../services/api';

const AdminPanel = ({ user }) => {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [testResults, setTestResults] = useState({});
  const [emailConfig, setEmailConfig] = useState(null);
  const [schedulerStatus, setSchedulerStatus] = useState(null);
  const [databases, setDatabases] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState(null);

  const ADMIN_PASSWORD = 'Vapaleon3';

  const handlePasswordSubmit = (e) => {
    e.preventDefault();
    if (password === ADMIN_PASSWORD) {
      setIsAuthenticated(true);
      loadInitialData();
    } else {
      alert('Неверный пароль');
      setPassword('');
    }
  };

  const loadInitialData = async () => {
    try {
      // Load email config
      const emailResponse = await api.get('/test/email-config');
      setEmailConfig(emailResponse.data);

      // Load scheduler status
      const schedulerResponse = await api.get('/scheduler/status');
      setSchedulerStatus(schedulerResponse.data.scheduler_status);

      // Load databases list (this is lightweight - just gets the list, no connection test)
      const databasesResponse = await api.get('/databases');
      setDatabases(databasesResponse.data);

      // Database connections will only be tested when button is pressed
    } catch (error) {
      console.error('Error loading initial data:', error);
    }
  };

  const testEmailNotifications = async () => {
    setLoading(true);
    try {
      const response = await api.post('/test/email-notifications');
      setTestResults(prev => ({
        ...prev,
        email: response.data
      }));
    } catch (error) {
      setTestResults(prev => ({
        ...prev,
        email: { 
          status: '❌ ERROR', 
          message: error.response?.data?.message || error.message 
        }
      }));
    }
    setLoading(false);
  };

  const testDailyDistribution = async () => {
    setLoading(true);
    try {
      const response = await api.post('/scheduler/test-distribution');
      setTestResults(prev => ({
        ...prev,
        dailyDistribution: response.data
      }));
    } catch (error) {
      setTestResults(prev => ({
        ...prev,
        dailyDistribution: { 
          success: false, 
          error: error.response?.data?.error || error.message 
        }
      }));
    }
    setLoading(false);
  };

  const testDatabaseConnections = async () => {
    setLoading(true);
    try {
      const response = await api.post('/databases/test-all-connections');
      setConnectionStatus(response.data);
      setTestResults(prev => ({
        ...prev,
        database: response.data
      }));
    } catch (error) {
      const errorData = {
        overall_status: 'error',
        message: error.response?.data?.message || error.message,
        dssb_app: { status: 'error', connected: false },
        spss: { status: 'error', connected: false }
      };
      setConnectionStatus(errorData);
      setTestResults(prev => ({
        ...prev,
        database: errorData
      }));
    }
    setLoading(false);
  };

  if (!isAuthenticated) {
    return (
      <div style={{ 
        padding: '2rem',
        maxWidth: '400px',
        margin: '0 auto',
        marginTop: '100px'
      }}>
        <div style={{
          backgroundColor: 'white',
          padding: '2rem',
          borderRadius: '8px',
          boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
          textAlign: 'center'
        }}>
          <Shield size={48} style={{ color: '#dc2626', marginBottom: '1rem' }} />
          <h2 style={{ marginBottom: '1rem', color: '#1f2937' }}>Панель администратора</h2>
          <p style={{ marginBottom: '1.5rem', color: '#6b7280' }}>
            Введите пароль для доступа к функциям администратора
          </p>
          
          <form onSubmit={handlePasswordSubmit}>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Пароль администратора"
              style={{
                width: '100%',
                padding: '0.75rem',
                marginBottom: '1rem',
                border: '1px solid #d1d5db',
                borderRadius: '6px',
                fontSize: '1rem'
              }}
              autoFocus
            />
            <button
              type="submit"
              style={{
                width: '100%',
                padding: '0.75rem',
                backgroundColor: '#dc2626',
                color: 'white',
                border: 'none',
                borderRadius: '6px',
                fontSize: '1rem',
                fontWeight: '600',
                cursor: 'pointer'
              }}
            >
              Войти
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: '1.5rem' }}>
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        marginBottom: '2rem',
        paddingBottom: '1rem',
        borderBottom: '2px solid #e5e7eb'
      }}>
        <Shield size={32} style={{ color: '#dc2626', marginRight: '1rem' }} />
        <div>
          <h1 style={{ margin: 0, color: '#1f2937' }}>Панель администратора</h1>
          <p style={{ margin: 0, color: '#6b7280', fontSize: '0.875rem' }}>
            Тестирование системных функций • Пользователь: {user?.username}
          </p>
        </div>
      </div>

      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', 
        gap: '1.5rem' 
      }}>
        
        {/* Email Testing Section */}
        <div style={{
          backgroundColor: 'white',
          padding: '1.5rem',
          borderRadius: '8px',
          boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
          border: '1px solid #e5e7eb'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
            <Mail size={24} style={{ color: '#3b82f6', marginRight: '0.5rem' }} />
            <h3 style={{ margin: 0 }}>Тестирование Email</h3>
          </div>
          
          {emailConfig && (
            <div style={{ 
              backgroundColor: '#f8fafc', 
              padding: '1rem', 
              borderRadius: '6px', 
              marginBottom: '1rem',
              fontSize: '0.875rem'
            }}>
              <p><strong>Отправитель:</strong> {emailConfig.email_sender}</p>
              <p><strong>SMTP сервер:</strong> {emailConfig.smtp_server}:{emailConfig.smtp_port}</p>
              <p><strong>Получатели:</strong> {emailConfig.recipients_count} адресов</p>
            </div>
          )}

          <button
            onClick={testEmailNotifications}
            disabled={loading}
            style={{
              width: '100%',
              padding: '0.75rem',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.5rem',
              opacity: loading ? 0.7 : 1
            }}
          >
            {loading ? <Clock size={16} /> : <Send size={16} />}
            {loading ? 'Отправка...' : 'Отправить тестовое письмо'}
          </button>

          {testResults.email && (
            <div style={{ 
              marginTop: '1rem', 
              padding: '1rem', 
              backgroundColor: testResults.email.status?.includes('SUCCESS') ? '#f0fdf4' : '#fef2f2',
              borderRadius: '6px',
              border: `1px solid ${testResults.email.status?.includes('SUCCESS') ? '#bbf7d0' : '#fecaca'}`
            }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '0.5rem' }}>
                {testResults.email.status?.includes('SUCCESS') ? 
                  <CheckCircle size={16} style={{ color: '#059669', marginRight: '0.5rem' }} /> :
                  <AlertCircle size={16} style={{ color: '#dc2626', marginRight: '0.5rem' }} />
                }
                <strong>{testResults.email.status}</strong>
              </div>
              <p style={{ margin: 0, fontSize: '0.875rem' }}>{testResults.email.message}</p>
            </div>
          )}
        </div>

        {/* Database Connections Section */}
        <div style={{
          backgroundColor: 'white',
          padding: '1.5rem',
          borderRadius: '8px',
          boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
          border: '1px solid #e5e7eb'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
            <Database size={24} style={{ color: '#7c3aed', marginRight: '0.5rem' }} />
            <h3 style={{ margin: 0 }}>Подключения к БД</h3>
          </div>

          {/* Available Databases */}
          {databases.length > 0 && (
            <div style={{ 
              backgroundColor: '#f8fafc', 
              padding: '1rem', 
              borderRadius: '6px', 
              marginBottom: '1rem',
              fontSize: '0.875rem'
            }}>
              <h4 style={{ margin: '0 0 0.5rem 0' }}>Доступные базы данных:</h4>
              {databases.map((db, index) => (
                <div key={index} style={{ marginBottom: '0.5rem' }}>
                  <strong>{db.name}</strong>
                  <p style={{ margin: 0, color: '#6b7280' }}>{db.description}</p>
                </div>
              ))}
            </div>
          )}

          {/* Connection Status */}
          {connectionStatus ? (
            <div style={{ 
              backgroundColor: '#f8fafc', 
              padding: '1rem', 
              borderRadius: '6px', 
              marginBottom: '1rem',
              fontSize: '0.875rem'
            }}>
              <h4 style={{ margin: '0 0 0.5rem 0' }}>Статус подключений:</h4>
              
              <div style={{ marginBottom: '0.5rem' }}>
                <strong>DSSB_APP:</strong> 
                <span style={{ 
                  color: connectionStatus.dssb_app?.connected ? '#059669' : '#dc2626',
                  marginLeft: '0.5rem'
                }}>
                  {connectionStatus.dssb_app?.connected ? '✅ Подключено' : '❌ Отключено'}
                </span>
              </div>

              <div style={{ marginBottom: '0.5rem' }}>
                <strong>SPSS:</strong> 
                <span style={{ 
                  color: connectionStatus.spss?.connected ? '#059669' : '#dc2626',
                  marginLeft: '0.5rem'
                }}>
                  {connectionStatus.spss?.connected ? '✅ Подключено' : '❌ Отключено'}
                </span>
              </div>

              <div>
                <strong>Общий статус:</strong> 
                <span style={{ 
                  color: connectionStatus.overall_status === 'success' ? '#059669' : 
                        connectionStatus.overall_status === 'partial' ? '#f59e0b' : '#dc2626',
                  marginLeft: '0.5rem'
                }}>
                  {connectionStatus.overall_status === 'success' ? '✅ Все подключения работают' :
                   connectionStatus.overall_status === 'partial' ? '⚠️ Частичное подключение' :
                   '❌ Проблемы с подключением'}
                </span>
              </div>
            </div>
          ) : (
            <div style={{ 
              backgroundColor: '#f9fafb', 
              padding: '1rem', 
              borderRadius: '6px', 
              marginBottom: '1rem',
              fontSize: '0.875rem',
              textAlign: 'center',
              color: '#6b7280'
            }}>
              <p style={{ margin: 0 }}>
                🔍 Нажмите кнопку ниже для проверки подключений к базам данных
              </p>
            </div>
          )}

          <button
            onClick={testDatabaseConnections}
            disabled={loading}
            style={{
              width: '100%',
              padding: '0.75rem',
              backgroundColor: '#7c3aed',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.5rem',
              opacity: loading ? 0.7 : 1
            }}
          >
            {loading ? <Clock size={16} /> : <Database size={16} />}
            {loading ? 'Тестирование...' : 'Тестировать подключения'}
          </button>

          {testResults.database && (
            <div style={{ 
              marginTop: '1rem', 
              padding: '1rem', 
              backgroundColor: testResults.database.overall_status === 'success' ? '#f0fdf4' : 
                              testResults.database.overall_status === 'partial' ? '#fef3c7' : '#fef2f2',
              borderRadius: '6px',
              border: `1px solid ${testResults.database.overall_status === 'success' ? '#bbf7d0' : 
                                   testResults.database.overall_status === 'partial' ? '#fbbf24' : '#fecaca'}`
            }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '0.5rem' }}>
                {testResults.database.overall_status === 'success' ? 
                  <CheckCircle size={16} style={{ color: '#059669', marginRight: '0.5rem' }} /> :
                  testResults.database.overall_status === 'partial' ?
                  <AlertCircle size={16} style={{ color: '#f59e0b', marginRight: '0.5rem' }} /> :
                  <AlertCircle size={16} style={{ color: '#dc2626', marginRight: '0.5rem' }} />
                }
                <strong>Результат тестирования</strong>
              </div>
              <div style={{ fontSize: '0.875rem' }}>
                <p><strong>DSSB_APP:</strong> {testResults.database.dssb_app?.message || 'Нет данных'}</p>
                <p><strong>SPSS:</strong> {testResults.database.spss?.message || 'Нет данных'}</p>
                <p><strong>Общий статус:</strong> {testResults.database.message}</p>
              </div>
            </div>
          )}
        </div>

        {/* Daily Distribution Testing Section */}
        <div style={{
          backgroundColor: 'white',
          padding: '1.5rem',
          borderRadius: '8px',
          boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
          border: '1px solid #e5e7eb'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
            <Calendar size={24} style={{ color: '#059669', marginRight: '0.5rem' }} />
            <h3 style={{ margin: 0 }}>Ежедневная дистрибуция</h3>
          </div>

          <button
            onClick={testDailyDistribution}
            disabled={loading}
            style={{
              width: '100%',
              padding: '0.75rem',
              backgroundColor: '#dc2626',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: loading ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.5rem',
              opacity: loading ? 0.7 : 1
            }}
          >
            <TestTube size={16} />
            Тестировать дистрибуцию
          </button>

          {testResults.dailyDistribution && (
            <div style={{ 
              marginTop: '1rem',
              padding: '1rem', 
              backgroundColor: testResults.dailyDistribution.success ? '#f0fdf4' : '#fef2f2',
              borderRadius: '6px',
              border: `1px solid ${testResults.dailyDistribution.success ? '#bbf7d0' : '#fecaca'}`
            }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '0.5rem' }}>
                {testResults.dailyDistribution.success ? 
                  <CheckCircle size={16} style={{ color: '#059669', marginRight: '0.5rem' }} /> :
                  <AlertCircle size={16} style={{ color: '#dc2626', marginRight: '0.5rem' }} />
                }
                <strong>
                  {testResults.dailyDistribution.success ? 'Тест выполнен' : 'Ошибка теста'}
                </strong>
              </div>
              {testResults.dailyDistribution.success ? (
                <div style={{ fontSize: '0.875rem' }}>
                  <p>{testResults.dailyDistribution.message}</p>
                  {testResults.dailyDistribution.test_result && (
                    <>
                      <p>• Кампаний найдено: {testResults.dailyDistribution.test_result.campaigns_found || 0}</p>
                      <p>• Пользователей найдено: {testResults.dailyDistribution.test_result.users_found || 0}</p>
                      <p>• Пользователей распределено: {testResults.dailyDistribution.test_result.users_distributed || 0}</p>
                    </>
                  )}
                </div>
              ) : (
                <p style={{ color: '#dc2626', fontSize: '0.875rem' }}>{testResults.dailyDistribution.error}</p>
              )}
            </div>
          )}

          <div style={{ 
            marginTop: '1rem', 
            padding: '1rem', 
            backgroundColor: '#fef3c7', 
            borderRadius: '6px',
            border: '1px solid #fbbf24'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', marginBottom: '0.5rem' }}>
              <AlertCircle size={16} style={{ color: '#f59e0b', marginRight: '0.5rem' }} />
              <strong style={{ color: '#f59e0b' }}>Важно</strong>
            </div>
            <p style={{ margin: 0, fontSize: '0.875rem', color: '#78350f' }}>
              Тестирование можно проводить в любое время, даже после 9:00 утра
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AdminPanel; 