import React, { useState, useEffect } from 'react';
import { Calendar, Users, Clock, AlertCircle, CheckCircle, Plus, History, Archive } from 'lucide-react';
import { databaseAPI } from '../services/api';

const ActiveTheories = ({ user }) => {
  const [theories, setTheories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeTab, setActiveTab] = useState('current'); // current, history
  const [filter, setFilter] = useState('all'); // all, active, inactive

  useEffect(() => {
    loadTheories();
  }, []);

  const loadTheories = async () => {
    try {
      setLoading(true);
      const response = await databaseAPI.getActiveTheories();
      setTheories(response.data);
      setError('');
    } catch (err) {
      console.error('Error loading theories:', err);
      setError('Ошибка загрузки кампаний');
    } finally {
      setLoading(false);
    }
  };

  // Split theories into current and historical based on end date
  const currentDate = new Date();
  const currentTheories = theories.filter(theory => {
    const endDate = new Date(theory.theory_end_date);
    return endDate >= currentDate;
  });

  const historicalTheories = theories.filter(theory => {
    const endDate = new Date(theory.theory_end_date);
    return endDate < currentDate;
  });

  // Get theories to display based on active tab
  const theoriesToShow = activeTab === 'current' ? currentTheories : historicalTheories;

  const filteredTheories = theoriesToShow.filter(theory => {
    if (filter === 'active') return theory.is_active;
    if (filter === 'inactive') return !theory.is_active;
    return true;
  });

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('ru-RU');
  };

  const getStatusIcon = (isActive) => {
    return isActive ? (
      <CheckCircle className="status-icon active" size={16} />
    ) : (
      <AlertCircle className="status-icon inactive" size={16} />
    );
  };

  const getStatusText = (theory) => {
    const now = new Date();
    const startDate = new Date(theory.theory_start_date);
    const endDate = new Date(theory.theory_end_date);

    if (now < startDate) {
      return 'Запланирована';
    } else if (now > endDate) {
      return 'Завершена';
    } else {
      return 'Активна';
    }
  };

  if (loading) {
    return (
      <div className="page-container">
        <div className="loading-state">
          <div className="loading-spinner"></div>
          <p>Загрузка теорий...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="page-container">
      <div className="page-header">
        <div className="page-title-section">
          <h1 className="page-title">🎯 Управление кампаниями</h1>
          <p className="page-description">
            Реестр кампаний и групп пользователей SoftCollection
          </p>
        </div>
      </div>

      {/* Tab Navigation */}
      <div className="tabs-container">
        <div className="tabs-header">
          <button
            className={`tab-btn ${activeTab === 'current' ? 'active' : ''}`}
            onClick={() => setActiveTab('current')}
          >
            <CheckCircle size={16} />
            Текущие кампании ({currentTheories.length})
          </button>
          <button
            className={`tab-btn ${activeTab === 'history' ? 'active' : ''}`}
            onClick={() => setActiveTab('history')}
          >
            <Archive size={16} />
            История кампаний ({historicalTheories.length})
          </button>
        </div>
      </div>

      {/* Filter Controls */}
      <div className="filter-section">
        <div className="filter-buttons">
          <button
            className={`filter-btn ${filter === 'all' ? 'active' : ''}`}
            onClick={() => setFilter('all')}
          >
            Все кампании ({theoriesToShow.length})
          </button>
          <button
            className={`filter-btn ${filter === 'active' ? 'active' : ''}`}
            onClick={() => setFilter('active')}
          >
            Активные ({theoriesToShow.filter(t => t.is_active).length})
          </button>
          <button
            className={`filter-btn ${filter === 'inactive' ? 'active' : ''}`}
            onClick={() => setFilter('inactive')}
          >
            Неактивные ({theoriesToShow.filter(t => !t.is_active).length})
          </button>
        </div>
      </div>

      {error && (
        <div className="alert alert-error">
          ⚠️ {error}
        </div>
      )}

      {/* Campaigns Grid */}
      <div className="theories-grid">
        {filteredTheories.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">
              {activeTab === 'current' ? '🎯' : '📚'}
            </div>
            <h3>
              {activeTab === 'current' 
                ? 'Текущие кампании не найдены' 
                : 'История кампаний пуста'
              }
            </h3>
            <p>
              {filter === 'all' 
                ? (activeTab === 'current' 
                    ? 'Пока нет активных кампаний. Создайте кампанию в Конструкторе запросов.'
                    : 'Нет завершенных кампаний.'
                  )
                : `Нет ${filter === 'active' ? 'активных' : 'неактивных'} кампаний в ${activeTab === 'current' ? 'текущем разделе' : 'истории'}.`
              }
            </p>
          </div>
        ) : (
          filteredTheories.map((theory) => (
            <div key={theory.theory_id} className={`theory-card ${activeTab === 'history' ? 'historical' : ''}`}>
              <div className="theory-header">
                <div className="theory-title">
                  <h3>{theory.theory_name}</h3>
                  <div className="theory-status">
                    {getStatusIcon(theory.is_active)}
                    <span className={`status-text ${theory.is_active ? 'active' : 'inactive'}`}>
                      {getStatusText(theory)}
                    </span>
                    {activeTab === 'history' && (
                      <span className="historical-badge">
                        <Archive size={12} />
                        Архив
                      </span>
                    )}
                  </div>
                </div>
                <div className="theory-id">
                  ID: {theory.theory_id}
                </div>
              </div>

              <div className="theory-description">
                <p>{theory.theory_description || 'Описание отсутствует'}</p>
              </div>

              <div className="theory-stats">
                <div className="theory-stat">
                  <Users size={16} />
                  <span>{theory.user_count} пользователей</span>
                </div>
                <div className="theory-stat">
                  <Calendar size={16} />
                  <span>{formatDate(theory.theory_start_date)} - {formatDate(theory.theory_end_date)}</span>
                </div>
                <div className="theory-stat">
                  <Clock size={16} />
                  <span>Создана: {formatDate(theory.load_date)}</span>
                </div>
              </div>

              <div className="theory-footer">
                <div className="theory-creator">
                  Создал: {theory.created_by}
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default ActiveTheories; 