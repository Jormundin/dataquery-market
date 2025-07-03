import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import QueryBuilder from './pages/QueryBuilder';
import DataViewer from './pages/DataViewer';
import ActiveTheories from './pages/ActiveTheories';
import CampaignManager from './pages/CampaignManager';
import Monitoring from './pages/Monitoring';
import Settings from './pages/Settings';
import AdminPanel from './pages/AdminPanel';
import Login from './pages/Login';
import { authAPI } from './services/api';
import './App.css';

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [user, setUser] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // Check authentication status on app load
    checkAuthStatus();
  }, []);

  const checkAuthStatus = async () => {
    try {
      if (authAPI.isAuthenticated()) {
        const userInfo = authAPI.getUserInfo();
        if (userInfo) {
          setUser(userInfo);
          setIsAuthenticated(true);
          
          // Verify token is still valid by calling /auth/me
          try {
            await authAPI.getCurrentUser();
          } catch (error) {
            // Token is invalid, logout
            handleLogout();
          }
        }
      }
    } catch (error) {
      console.error('Auth check failed:', error);
      handleLogout();
    } finally {
      setIsLoading(false);
    }
  };

  const handleLoginSuccess = (userInfo) => {
    setUser(userInfo);
    setIsAuthenticated(true);
  };

  const handleLogout = () => {
    authAPI.logout();
    setUser(null);
    setIsAuthenticated(false);
  };

  // Show loading screen while checking authentication
  if (isLoading) {
    return (
      <div className="loading-state">
        <div className="loading-spinner"></div>
        <p>Проверка аутентификации...</p>
      </div>
    );
  }

  // Show login page if not authenticated
  if (!isAuthenticated) {
    return <Login onLoginSuccess={handleLoginSuccess} />;
  }

  // Show main application if authenticated
  return (
    <Router>
      <div className="app-layout">
        <Layout user={user} onLogout={handleLogout} />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard user={user} />} />
            <Route path="/query-builder" element={<QueryBuilder user={user} />} />
            <Route path="/data-viewer" element={<DataViewer user={user} />} />
            <Route path="/active-theories" element={<ActiveTheories user={user} />} />
            <Route path="/campaign-manager" element={<CampaignManager user={user} />} />
            <Route path="/monitoring" element={<Monitoring user={user} />} />
            <Route path="/settings" element={<Settings user={user} />} />
            <Route path="/admin-panel" element={<AdminPanel user={user} />} />
            <Route path="*" element={<Navigate to="/dashboard" replace />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
