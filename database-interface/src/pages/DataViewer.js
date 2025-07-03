import React, { useState, useEffect } from 'react';
import { 
  Search, 
  Download, 
  RefreshCw, 
  Filter,
  ChevronLeft,
  ChevronRight,
  Eye
} from 'lucide-react';
import { dataAPI, databaseAPI } from '../services/api';

const DataViewer = () => {
  const [searchInput, setSearchInput] = useState(''); // Input value for search box
  const [searchTerm, setSearchTerm] = useState(''); // Actual search term used for API calls
  const [currentPage, setCurrentPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(100); // Increased from 25 to 100 for better performance
  const [sortColumn, setSortColumn] = useState('');
  const [sortDirection, setSortDirection] = useState('asc');
  const [selectedRows, setSelectedRows] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [data, setData] = useState([]);
  const [totalRows, setTotalRows] = useState(0); // Track total rows from server
  const [error, setError] = useState(null);
  const [selectedTable, setSelectedTable] = useState('');
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);

  // No default columns - will be loaded dynamically from actual table structure

  useEffect(() => {
    loadTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      loadTableColumns();
      loadData();
    }
  }, [selectedTable, currentPage, rowsPerPage, sortColumn, sortDirection, searchTerm]);

  const loadTables = async () => {
    try {
      const response = await databaseAPI.getDatabases();
      if (response.data && response.data.length > 0) {
        const tablesResponse = await databaseAPI.getTables(response.data[0].id);
        setTables(tablesResponse.data || []);
      }
    } catch (err) {
      console.error('Error loading tables:', err);
      setError('Ошибка загрузки таблиц');
    }
  };

  const loadTableColumns = async () => {
    try {
      const response = await databaseAPI.getTableColumns('DSSB_APP', selectedTable);
      const tableColumns = response.data || [];
      
      // Convert backend column format to frontend format
      const formattedColumns = tableColumns.map(col => ({
        key: col.name,
        label: col.description || col.name,
        type: mapOracleTypeToDisplayType(col.type)
      }));
      
      setColumns(formattedColumns);
    } catch (err) {
      console.error('Error loading table columns:', err);
      setError('Ошибка загрузки столбцов таблицы: ' + (err.response?.data?.detail || err.message));
      setColumns([]);
    }
  };

  const mapOracleTypeToDisplayType = (oracleType) => {
    const type = oracleType.toUpperCase();
    if (type.includes('NUMBER')) return 'number';
    if (type.includes('DATE')) return 'date';
    if (type.includes('VARCHAR')) return 'text';
    if (type.includes('CLOB')) return 'text';
    return 'text';
  };

  const loadData = async () => {
    if (!selectedTable) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const params = {
        database_id: 'dssb_app',
        table: selectedTable,
        page: currentPage,
        limit: rowsPerPage,
        search: searchTerm,
        sort_by: sortColumn,
        sort_order: sortDirection
      };

      const response = await dataAPI.getData(params);
      const responseData = response.data.data || [];
      const totalCount = response.data.total || responseData.length;
      
      setData(responseData);
      setTotalRows(totalCount);

      // If no columns are defined but we have data, create columns from the first row
      if (columns.length === 0 && responseData.length > 0) {
        const firstRow = responseData[0];
        const autoColumns = Object.keys(firstRow).map(key => ({
          key: key,
          label: key.toUpperCase(),
          type: 'text' // Default to text type
        }));
        setColumns(autoColumns);
      }
      
    } catch (err) {
      console.error('Error loading data:', err);
      setError('Ошибка загрузки данных: ' + (err.response?.data?.detail || err.message));
      
      // Set empty data when API fails - no dummy data
      setData([]);
      setTotalRows(0);
    } finally {
      setIsLoading(false);
    }
  };

  // Calculate pagination info based on server data
  const totalPages = Math.ceil(totalRows / rowsPerPage);
  const startIndex = (currentPage - 1) * rowsPerPage + 1;
  const endIndex = Math.min(currentPage * rowsPerPage, totalRows);

  const handleSort = (column) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
    setCurrentPage(1); // Reset to first page when sorting changes
  };

  const handleRowSelect = (id) => {
    setSelectedRows(prev =>
      prev.includes(id)
        ? prev.filter(rowId => rowId !== id)
        : [...prev, id]
    );
  };

  const handleSelectAll = () => {
    if (selectedRows.length === data.length) {
      setSelectedRows([]);
    } else {
      const allRowIds = data.map((row, index) => row.id || `row_${currentPage}_${index}`);
      setSelectedRows(allRowIds);
    }
  };

  // Handle search button click
  const handleSearch = () => {
    setSearchTerm(searchInput);
    setCurrentPage(1); // Reset to first page when searching
  };

  // Handle clear search
  const handleClearSearch = () => {
    setSearchInput('');
    setSearchTerm('');
    setCurrentPage(1);
  };

  // Handle Enter key press in search input
  const handleSearchKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  // Handle page change
  const handlePageChange = (newPage) => {
    setCurrentPage(newPage);
  };

  const formatCellValue = (value, type) => {
    // Handle null, undefined, or empty values
    if (value === null || value === undefined || value === '') {
      return <span style={{ color: '#9ca3af', fontStyle: 'italic' }}>—</span>;
    }

    switch (type) {
      case 'currency':
        if (typeof value === 'number') {
          return new Intl.NumberFormat('ru-RU', {
            style: 'currency',
            currency: 'RUB'
          }).format(value);
        }
        return value;
      case 'number':
        if (typeof value === 'number') {
          return new Intl.NumberFormat('ru-RU').format(value);
        }
        return value;
      case 'date':
        try {
          const date = new Date(value);
          if (isNaN(date.getTime())) {
            return value; // Return original value if not a valid date
          }
          return date.toLocaleDateString('ru-RU');
        } catch (error) {
          return value; // Return original value if date parsing fails
        }
      case 'badge':
        return (
          <span className={`badge ${value === 'active' ? 'badge-success' : 'badge-warning'}`}>
            {value === 'active' ? 'Активный' : 'Неактивный'}
          </span>
        );
      default:
        return String(value);
    }
  };

  const refreshData = async () => {
    await loadData();
  };

  const exportData = async () => {
    try {
      const response = await dataAPI.exportData({
        database_id: 'dssb_app',
        table: selectedTable,
        search: searchTerm,
        sort_by: sortColumn,
        sort_order: sortDirection
      });
      
      const url = window.URL.createObjectURL(response.data);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${selectedTable}_export.csv`;
      a.click();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Export error:', err);
      // Fallback to client-side export
      const csvContent = [
        columns.map(col => col.label).join(','),
        ...data.map(row =>
          columns.map(col => row[col.key]).join(',')
        )
      ].join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'export-data.csv';
      a.click();
      window.URL.revokeObjectURL(url);
    }
  };

  return (
    <div className="data-viewer">
      <div className="dashboard-header">
        <h1 className="dashboard-title">Просмотр данных</h1>
        <p className="dashboard-subtitle">
          Просматривайте и анализируйте записи вашей базы данных
        </p>
      </div>

      {error && (
        <div className="alert alert-error" style={{ marginBottom: '1.5rem' }}>
          {error}
        </div>
      )}

      {/* Table Selection */}
      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div className="card-header">
          <h2 className="card-title">Выбор таблицы</h2>
        </div>
        <div className="form-group">
          <select 
            className="form-select"
            value={selectedTable}
            onChange={(e) => {
              setSelectedTable(e.target.value);
              setCurrentPage(1);
              setSelectedRows([]);
              // Clear search when changing tables
              setSearchInput('');
              setSearchTerm('');
            }}
          >
            <option value="">Выберите таблицу для просмотра...</option>
            {tables.map(table => (
              <option key={table.name} value={table.name}>
                {table.name} - {table.description}
              </option>
            ))}
          </select>
        </div>
      </div>

      {selectedTable && (
        <>
          {/* Controls */}
          <div className="card">
            <div className="card-header">
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flex: 1 }}>
                  {/* Search Section */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', minWidth: '350px' }}>
                    <div style={{ position: 'relative', flex: 1 }}>
                      <Search 
                        className="search-icon" 
                        style={{ 
                          position: 'absolute', 
                          left: '0.75rem', 
                          top: '50%', 
                          transform: 'translateY(-50%)', 
                          width: '16px', 
                          height: '16px', 
                          color: '#6b7280' 
                        }} 
                      />
                      <input
                        type="text"
                        placeholder="Поиск по всем столбцам..."
                        className="form-input"
                        style={{ paddingLeft: '2.5rem' }}
                        value={searchInput}
                        onChange={(e) => setSearchInput(e.target.value)}
                        onKeyPress={handleSearchKeyPress}
                      />
                    </div>
                    <button 
                      className="btn btn-primary"
                      onClick={handleSearch}
                      disabled={isLoading}
                      style={{ whiteSpace: 'nowrap' }}
                    >
                      <Search className="nav-icon" style={{ width: '16px', height: '16px' }} />
                      Поиск
                    </button>
                    {searchTerm && (
                      <button 
                        className="btn btn-secondary"
                        onClick={handleClearSearch}
                        disabled={isLoading}
                        style={{ whiteSpace: 'nowrap' }}
                      >
                        Очистить
                      </button>
                    )}
                  </div>
                  
                  <select
                    className="form-select"
                    value={rowsPerPage}
                    onChange={(e) => {
                      setRowsPerPage(parseInt(e.target.value));
                      setCurrentPage(1);
                    }}
                    style={{ width: 'auto' }}
                  >
                    <option value={10}>10 строк</option>
                    <option value={25}>25 строк</option>
                    <option value={50}>50 строк</option>
                    <option value={100}>100 строк</option>
                  </select>
                </div>

                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button 
                    className="btn btn-secondary"
                    onClick={refreshData}
                    disabled={isLoading}
                  >
                    <RefreshCw 
                      className="nav-icon" 
                      style={{ 
                        width: '16px', 
                        height: '16px',
                        animation: isLoading ? 'spin 1s linear infinite' : 'none'
                      }} 
                    />
                    Обновить
                  </button>
                  
                  <button 
                    className="btn btn-success"
                    onClick={exportData}
                    disabled={!selectedTable}
                  >
                    <Download className="nav-icon" style={{ width: '16px', height: '16px' }} />
                    Экспорт
                  </button>
                </div>
              </div>
            </div>

            {/* Statistics */}
            <div style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center', 
              padding: '1rem', 
              backgroundColor: '#f9fafb', 
              borderRadius: '6px',
              fontSize: '0.875rem',
              color: '#6b7280'
            }}>
              <div>
                Показано {startIndex} до {endIndex} из {totalRows} записей
                {searchTerm && ` (поиск: "${searchTerm}")`}
              </div>
              {selectedRows.length > 0 && (
                <div>
                  {selectedRows.length} {selectedRows.length === 1 ? 'строка выбрана' : selectedRows.length < 5 ? 'строки выбрано' : 'строк выбрано'}
                </div>
              )}
            </div>
          </div>

          {/* Data Table */}
          {columns.length > 0 ? (
            <div className="table-container">
              <table className="table">
              <thead>
                <tr>
                  <th style={{ width: '50px' }}>
                    <input
                      type="checkbox"
                      checked={selectedRows.length === data.length && data.length > 0}
                      onChange={handleSelectAll}
                    />
                  </th>
                  {columns.map((column) => (
                    <th
                      key={column.key}
                      style={{ cursor: 'pointer', userSelect: 'none' }}
                      onClick={() => handleSort(column.key)}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        {column.label}
                        {sortColumn === column.key && (
                          <span style={{ fontSize: '0.75rem' }}>
                            {sortDirection === 'asc' ? '↑' : '↓'}
                          </span>
                        )}
                      </div>
                    </th>
                  ))}
                  <th style={{ width: '100px' }}>Действия</th>
                </tr>
              </thead>
              <tbody>
                {data.length > 0 ? (
                  data.map((row, index) => {
                    const rowId = row.id || `row_${currentPage}_${index}`;
                    return (
                      <tr key={rowId}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedRows.includes(rowId)}
                            onChange={() => handleRowSelect(rowId)}
                          />
                        </td>
                        {columns.map((column) => (
                          <td key={column.key}>
                            {formatCellValue(row[column.key], column.type)}
                          </td>
                        ))}
                        <td>
                          <button
                            className="btn btn-secondary"
                            style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}
                          >
                            <Eye style={{ width: '14px', height: '14px' }} />
                          </button>
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={columns.length + 2} style={{ textAlign: 'center', padding: '3rem', color: '#6b7280' }}>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem' }}>
                        <Filter size={48} style={{ opacity: 0.3 }} />
                        <div>
                          <p style={{ margin: 0, fontSize: '1.1rem', fontWeight: '500' }}>
                            {searchTerm ? 'Данные не найдены' : 'Таблица пуста'}
                          </p>
                          <small style={{ opacity: 0.7 }}>
                            {searchTerm 
                              ? 'Попробуйте изменить критерии поиска' 
                              : 'В выбранной таблице нет данных'
                            }
                          </small>
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            </div>
          ) : (
            <div className="card" style={{ textAlign: 'center', padding: '3rem', color: '#6b7280' }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem' }}>
                <Filter size={48} style={{ opacity: 0.3 }} />
                <div>
                  <p style={{ margin: 0, fontSize: '1.1rem', fontWeight: '500' }}>
                    Таблица не выбрана
                  </p>
                  <small style={{ opacity: 0.7 }}>
                    Выберите таблицу из списка выше для просмотра данных
                  </small>
                </div>
              </div>
            </div>
          )}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="card">
              <div style={{ 
                display: 'flex', 
                justifyContent: 'space-between', 
                alignItems: 'center', 
                padding: '1rem' 
              }}>
                <div style={{ fontSize: '0.875rem', color: '#6b7280' }}>
                  Страница {currentPage} из {totalPages}
                </div>
                
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button
                    className="btn btn-secondary"
                    onClick={() => handlePageChange(Math.max(currentPage - 1, 1))}
                    disabled={currentPage === 1}
                  >
                    <ChevronLeft style={{ width: '16px', height: '16px' }} />
                    Назад
                  </button>
                  
                  {/* Page numbers */}
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                    let pageNum;
                    if (totalPages <= 5) {
                      pageNum = i + 1;
                    } else if (currentPage <= 3) {
                      pageNum = i + 1;
                    } else if (currentPage >= totalPages - 2) {
                      pageNum = totalPages - 4 + i;
                    } else {
                      pageNum = currentPage - 2 + i;
                    }
                    
                    return (
                      <button
                        key={pageNum}
                        className={`btn ${currentPage === pageNum ? 'btn-primary' : 'btn-secondary'}`}
                        onClick={() => handlePageChange(pageNum)}
                        style={{ minWidth: '40px' }}
                      >
                        {pageNum}
                      </button>
                    );
                  })}
                  
                  <button
                    className="btn btn-secondary"
                    onClick={() => handlePageChange(Math.min(currentPage + 1, totalPages))}
                    disabled={currentPage === totalPages}
                  >
                    Вперед
                    <ChevronRight style={{ width: '16px', height: '16px' }} />
                  </button>
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default DataViewer; 