import React, { useState } from 'react';
import { databaseAPI } from '../services/api';

const Login = ({ onLoginSuccess }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username || !password) {
      setError('Пожалуйста, введите имя пользователя и пароль');
      return;
    }

    setIsLoading(true);
    setError('');

    try {
      const response = await databaseAPI.login({ username, password });
      
      // Store token and user info
      localStorage.setItem('authToken', response.data.access_token);
      localStorage.setItem('userInfo', JSON.stringify(response.data.user));
      
      // Call success callback
      onLoginSuccess(response.data.user);
      
    } catch (err) {
      console.error('Login error:', err);
      if (err.response?.status === 401) {
        setError('The username or password you have entered is incorrect.');
      } else if (err.response?.status === 403) {
        setError('Доступ к ресурсу отсутствует');
      } else {
        setError('Ошибка подключения к серверу');
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div style={{ 
      minHeight: '100vh', 
      display: 'flex', 
      alignItems: 'center', 
      justifyContent: 'center',
      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
    }}>
      <div style={{
        background: 'white',
        padding: '3rem',
        borderRadius: '10px',
        boxShadow: '0 20px 60px rgba(0, 0, 0, 0.1)',
        width: '100%',
        maxWidth: '400px'
      }}>
        <div style={{ textAlign: 'center', marginBottom: '2rem' }}>
          <h1 style={{ 
            color: '#1f2937', 
            fontSize: '2rem', 
            fontWeight: '700', 
            marginBottom: '0.5rem' 
          }}>
            🏢 OCDS-SoftCollection
          </h1>
          <p style={{ color: '#6b7280', fontSize: '1rem' }}>
            Корпоративный интерфейс для Oracle Database
          </p>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label className="form-label">Имя пользователя</label>
            <input
              type="text"
              className="form-input"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Введите ваш логин"
              autoComplete="username"
              disabled={isLoading}
            />
          </div>

          <div className="form-group">
            <label className="form-label">Пароль</label>
            <input
              type="password"
              className="form-input"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Введите ваш пароль"
              autoComplete="current-password"
              disabled={isLoading}
            />
          </div>

          {error && (
            <div className="alert alert-error" style={{ marginBottom: '1rem' }}>
              ⚠️ {error}
            </div>
          )}

          <button
            type="submit"
            className="btn btn-primary"
            disabled={isLoading}
            style={{ 
              width: '100%', 
              padding: '0.875rem',
              fontSize: '1rem',
              fontWeight: '600'
            }}
          >
            {isLoading ? (
              <>
                <div className="loading-spinner" style={{ 
                  width: '16px', 
                  height: '16px', 
                  marginRight: '0.5rem',
                  borderWidth: '2px'
                }}></div>
                Подключение...
              </>
            ) : (
              'Войти в систему'
            )}
          </button>
        </form>

        <div style={{ 
          marginTop: '2rem', 
          textAlign: 'center', 
          fontSize: '0.875rem', 
          color: '#6b7280' 
        }}>
        </div>
      </div>
    </div>
  );
};

export default Login; 