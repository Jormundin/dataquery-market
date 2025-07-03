import React, { useState } from 'react';
import { 
  Database, 
  Server, 
  Save, 
  TestTube, 
  AlertCircle,
  CheckCircle,
  Settings as SettingsIcon,
  User
} from 'lucide-react';

const Settings = () => {
  const [activeTab, setActiveTab] = useState('database');
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [isTestingConnection, setIsTestingConnection] = useState(false);

  const [databaseConfig, setDatabaseConfig] = useState({
    host: 'your-oracle-server.company.com',
    port: '1521',
    database: 'PROD', // Oracle Service Name or SID
    username: 'db_user',
    password: '',
    ssl: false,
    connectionTimeout: 30
  });

  const [apiConfig, setApiConfig] = useState({
    baseUrl: 'http://localhost:8000',
    timeout: 30000,
    retries: 3,
    apiKey: ''
  });

  const [userPreferences, setUserPreferences] = useState({
    defaultRowsPerPage: 25,
    dateFormat: 'dd.MM.yyyy',
    timezone: 'Europe/Moscow',
    theme: 'light',
    autoRefresh: false,
    refreshInterval: 30
  });

  const testConnection = async () => {
    setIsTestingConnection(true);
    setConnectionStatus('testing');
    
    // Simulate connection test
    setTimeout(() => {
      const success = databaseConfig.host && databaseConfig.database && databaseConfig.username;
      setConnectionStatus(success ? 'connected' : 'failed');
      setIsTestingConnection(false);
    }, 2000);
  };

  const saveSettings = () => {
    // Save settings logic here
    alert('Настройки успешно сохранены!');
  };

  const tabs = [
    { id: 'database', label: 'Подключение к БД', icon: Database },
    { id: 'api', label: 'Настройки API', icon: Server },
    { id: 'preferences', label: 'Пользовательские настройки', icon: User },
  ];

  const renderConnectionStatus = () => {
    switch (connectionStatus) {
      case 'connected':
        return (
          <div style={{ display: 'flex', alignItems: 'center', color: '#10b981', fontSize: '0.875rem' }}>
            <CheckCircle style={{ width: '16px', height: '16px', marginRight: '0.5rem' }} />
            Соединение установлено
          </div>
        );
      case 'failed':
        return (
          <div style={{ display: 'flex', alignItems: 'center', color: '#ef4444', fontSize: '0.875rem' }}>
            <AlertCircle style={{ width: '16px', height: '16px', marginRight: '0.5rem' }} />
            Ошибка соединения
          </div>
        );
      case 'testing':
        return (
          <div style={{ display: 'flex', alignItems: 'center', color: '#f59e0b', fontSize: '0.875rem' }}>
            <TestTube style={{ width: '16px', height: '16px', marginRight: '0.5rem' }} />
            Проверка соединения...
          </div>
        );
      default:
        return (
          <div style={{ display: 'flex', alignItems: 'center', color: '#6b7280', fontSize: '0.875rem' }}>
            <AlertCircle style={{ width: '16px', height: '16px', marginRight: '0.5rem' }} />
            Не подключено
          </div>
        );
    }
  };

  return (
    <div className="settings">
      <div className="dashboard-header">
        <h1 className="dashboard-title">Настройки</h1>
        <p className="dashboard-subtitle">
          Настройте подключения к базам данных и пользовательские предпочтения
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '250px 1fr', gap: '1.5rem' }}>
        {/* Sidebar Navigation */}
        <div className="card" style={{ height: 'fit-content' }}>
          <div className="card-header">
            <h2 className="card-title">
              <SettingsIcon className="nav-icon" style={{ width: '20px', height: '20px', display: 'inline', marginRight: '0.5rem' }} />
              Конфигурация
            </h2>
          </div>
          
          <nav>
            <ul style={{ listStyle: 'none', margin: 0, padding: 0 }}>
              {tabs.map((tab) => {
                const Icon = tab.icon;
                return (
                  <li key={tab.id} style={{ marginBottom: '0.5rem' }}>
                    <button
                      className={`nav-link ${activeTab === tab.id ? 'active' : ''}`}
                      onClick={() => setActiveTab(tab.id)}
                      style={{
                        width: '100%',
                        textAlign: 'left',
                        border: 'none',
                        background: activeTab === tab.id ? '#3b82f6' : 'transparent',
                        color: activeTab === tab.id ? 'white' : '#6b7280',
                        padding: '0.75rem',
                        borderRadius: '6px',
                        display: 'flex',
                        alignItems: 'center'
                      }}
                    >
                      <Icon className="nav-icon" />
                      {tab.label}
                    </button>
                  </li>
                );
              })}
            </ul>
          </nav>
        </div>

        {/* Main Content */}
        <div>
          {activeTab === 'database' && (
            <div className="card">
              <div className="card-header">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <h2 className="card-title">Подключение к базе данных</h2>
                  {renderConnectionStatus()}
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                <div className="form-group">
                  <label className="form-label">Хост</label>
                  <input
                    type="text"
                    className="form-input"
                    value={databaseConfig.host}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, host: e.target.value})}
                    placeholder="localhost"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Порт</label>
                  <input
                    type="number"
                    className="form-input"
                    value={databaseConfig.port}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, port: e.target.value})}
                    placeholder="1521"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Имя базы данных</label>
                  <input
                    type="text"
                    className="form-input"
                    value={databaseConfig.database}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, database: e.target.value})}
                    placeholder="PROD"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Имя пользователя</label>
                  <input
                    type="text"
                    className="form-input"
                    value={databaseConfig.username}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, username: e.target.value})}
                    placeholder="db_user"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Пароль</label>
                  <input
                    type="password"
                    className="form-input"
                    value={databaseConfig.password}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, password: e.target.value})}
                    placeholder="••••••••"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Тайм-аут соединения (секунды)</label>
                  <input
                    type="number"
                    className="form-input"
                    value={databaseConfig.connectionTimeout}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, connectionTimeout: parseInt(e.target.value)})}
                    min="5"
                    max="300"
                  />
                </div>
              </div>

              <div className="form-group">
                <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <input
                    type="checkbox"
                    checked={databaseConfig.ssl}
                    onChange={(e) => setDatabaseConfig({...databaseConfig, ssl: e.target.checked})}
                  />
                  Включить SSL соединение
                </label>
              </div>

              <div style={{ display: 'flex', gap: '1rem', marginTop: '1.5rem' }}>
                <button
                  className="btn btn-primary"
                  onClick={testConnection}
                  disabled={isTestingConnection}
                >
                  <TestTube className="nav-icon" style={{ width: '16px', height: '16px' }} />
                  {isTestingConnection ? 'Проверка...' : 'Проверить соединение'}
                </button>
                
                <button className="btn btn-success" onClick={saveSettings}>
                  <Save className="nav-icon" style={{ width: '16px', height: '16px' }} />
                  Сохранить конфигурацию
                </button>
              </div>
            </div>
          )}

          {activeTab === 'api' && (
            <div className="card">
              <div className="card-header">
                <h2 className="card-title">Настройки API</h2>
              </div>

              <div className="form-group">
                <label className="form-label">Базовый URL</label>
                <input
                  type="url"
                  className="form-input"
                  value={apiConfig.baseUrl}
                  onChange={(e) => setApiConfig({...apiConfig, baseUrl: e.target.value})}
                  placeholder="http://localhost:8000"
                />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                <div className="form-group">
                  <label className="form-label">Тайм-аут (мс)</label>
                  <input
                    type="number"
                    className="form-input"
                    value={apiConfig.timeout}
                    onChange={(e) => setApiConfig({...apiConfig, timeout: parseInt(e.target.value)})}
                    min="1000"
                    max="300000"
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Максимум повторов</label>
                  <input
                    type="number"
                    className="form-input"
                    value={apiConfig.retries}
                    onChange={(e) => setApiConfig({...apiConfig, retries: parseInt(e.target.value)})}
                    min="0"
                    max="10"
                  />
                </div>
              </div>

              <div className="form-group">
                <label className="form-label">API ключ</label>
                <input
                  type="password"
                  className="form-input"
                  value={apiConfig.apiKey}
                  onChange={(e) => setApiConfig({...apiConfig, apiKey: e.target.value})}
                  placeholder="Введите ваш API ключ..."
                />
              </div>

              <button className="btn btn-success" onClick={saveSettings}>
                <Save className="nav-icon" style={{ width: '16px', height: '16px' }} />
                Сохранить настройки API
              </button>
            </div>
          )}

          {activeTab === 'preferences' && (
            <div className="card">
              <div className="card-header">
                <h2 className="card-title">Пользовательские настройки</h2>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                <div className="form-group">
                  <label className="form-label">Строк на странице по умолчанию</label>
                  <select
                    className="form-select"
                    value={userPreferences.defaultRowsPerPage}
                    onChange={(e) => setUserPreferences({...userPreferences, defaultRowsPerPage: parseInt(e.target.value)})}
                  >
                    <option value={10}>10 строк</option>
                    <option value={25}>25 строк</option>
                    <option value={50}>50 строк</option>
                    <option value={100}>100 строк</option>
                  </select>
                </div>

                <div className="form-group">
                  <label className="form-label">Формат даты</label>
                  <select
                    className="form-select"
                    value={userPreferences.dateFormat}
                    onChange={(e) => setUserPreferences({...userPreferences, dateFormat: e.target.value})}
                  >
                    <option value="dd.MM.yyyy">ДД.ММ.ГГГГ</option>
                    <option value="MM/dd/yyyy">ММ/ДД/ГГГГ</option>
                    <option value="yyyy-MM-dd">ГГГГ-ММ-ДД</option>
                  </select>
                </div>

                <div className="form-group">
                  <label className="form-label">Часовой пояс</label>
                  <select
                    className="form-select"
                    value={userPreferences.timezone}
                    onChange={(e) => setUserPreferences({...userPreferences, timezone: e.target.value})}
                  >
                    <option value="Europe/Moscow">Московское время</option>
                    <option value="Europe/Minsk">Минское время</option>
                    <option value="Europe/Kiev">Киевское время</option>
                    <option value="Asia/Almaty">Алматинское время</option>
                    <option value="UTC">UTC</option>
                  </select>
                </div>

                <div className="form-group">
                  <label className="form-label">Тема</label>
                  <select
                    className="form-select"
                    value={userPreferences.theme}
                    onChange={(e) => setUserPreferences({...userPreferences, theme: e.target.value})}
                  >
                    <option value="light">Светлая</option>
                    <option value="dark">Темная</option>
                    <option value="auto">Автоматически</option>
                  </select>
                </div>
              </div>

              <div className="form-group">
                <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <input
                    type="checkbox"
                    checked={userPreferences.autoRefresh}
                    onChange={(e) => setUserPreferences({...userPreferences, autoRefresh: e.target.checked})}
                  />
                  Включить автообновление
                </label>
              </div>

              {userPreferences.autoRefresh && (
                <div className="form-group">
                  <label className="form-label">Интервал обновления (секунды)</label>
                  <input
                    type="number"
                    className="form-input"
                    value={userPreferences.refreshInterval}
                    onChange={(e) => setUserPreferences({...userPreferences, refreshInterval: parseInt(e.target.value)})}
                    min="10"
                    max="3600"
                  />
                </div>
              )}

              <button className="btn btn-success" onClick={saveSettings}>
                <Save className="nav-icon" style={{ width: '16px', height: '16px' }} />
                Сохранить настройки
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Settings; 