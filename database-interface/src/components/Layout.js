import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  BarChart3, 
  Database, 
  Eye, 
  Settings, 
  User, 
  LogOut,
  Shield,
  TestTube,
  Activity,
  Target
} from 'lucide-react';

const Layout = ({ user, onLogout, children }) => {
  const location = useLocation();

  const navigationItems = [
    { path: '/dashboard', label: 'Панель управления', icon: BarChart3 },
    { path: '/query-builder', label: 'Конструктор запросов', icon: Database },
    { path: '/data-viewer', label: 'Просмотр данных', icon: Eye },
    { path: '/active-theories', label: 'Активные теории', icon: TestTube },
    { path: '/campaign-manager', label: 'Управление кампаниями', icon: Target },
    { path: '/monitoring', label: 'Мониторинг системы', icon: Activity },
    { path: '/settings', label: 'Настройки', icon: Settings },
    { path: '/admin-panel', label: 'Панель администратора', icon: Shield },
  ];

  const handleLogout = () => {
    if (window.confirm('Вы уверены, что хотите выйти из системы?')) {
      onLogout();
    }
  };

  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="sidebar-header">
          <h1 className="sidebar-title">OCDS-SoftCollection</h1>
          <p style={{ fontSize: '0.875rem', color: '#94a3b8', marginTop: '0.5rem' }}>
            Oracle Database Interface
          </p>
        </div>

        <nav className="nav-menu">
          {navigationItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;
            
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`nav-link ${isActive ? 'active' : ''}`}
              >
                <Icon className="nav-icon" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* User Info Section */}
        <div style={{ 
          marginTop: 'auto', 
          padding: '1.5rem 2rem 2rem 2rem',
          borderTop: '1px solid #334155'
        }}>
          {/* User Profile */}
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            marginBottom: '1rem',
            padding: '0.75rem',
            backgroundColor: '#334155',
            borderRadius: '8px'
          }}>
            <div style={{
              width: '40px',
              height: '40px',
              backgroundColor: '#3b82f6',
              borderRadius: '50%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              marginRight: '0.75rem'
            }}>
              <User size={20} color="white" />
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ 
                color: 'white', 
                fontWeight: '600',
                fontSize: '0.875rem',
                lineHeight: '1.2'
              }}>
                {user?.name || 'User'}
              </div>
              <div style={{ 
                color: '#94a3b8', 
                fontSize: '0.75rem',
                lineHeight: '1.2'
              }}>
                {user?.username}
              </div>
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                marginTop: '0.25rem'
              }}>
                <Shield size={12} style={{ marginRight: '0.25rem', color: '#10b981' }} />
                <span style={{ 
                  color: '#10b981', 
                  fontSize: '0.75rem',
                  textTransform: 'capitalize'
                }}>
                  {user?.role || 'user'}
                </span>
              </div>
            </div>
          </div>

          {/* Authentication Info */}
          <div style={{ 
            fontSize: '0.75rem', 
            color: '#94a3b8', 
            marginBottom: '1rem',
            textAlign: 'center'
          }}>
            🔐 LDAP Authentication
          </div>

          {/* Logout Button */}
          <button
            onClick={handleLogout}
            style={{
              width: '100%',
              padding: '0.75rem',
              backgroundColor: '#dc2626',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              fontSize: '0.875rem',
              fontWeight: '500',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.5rem',
              transition: 'background-color 0.2s ease'
            }}
            onMouseOver={(e) => e.target.style.backgroundColor = '#b91c1c'}
            onMouseOut={(e) => e.target.style.backgroundColor = '#dc2626'}
          >
            <LogOut size={16} />
            Выйти из системы
          </button>
        </div>
      </aside>

      <main className="main-content">
        {children}
      </main>
    </div>
  );
};

export default Layout; 