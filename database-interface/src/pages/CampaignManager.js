import React, { useState, useEffect } from 'react';
import { campaignAPI, parquetAPI, fileAPI } from '../services/api';

const CampaignManager = ({ user }) => {
  // State management
  const [activeTab, setActiveTab] = useState('list');
  const [campaigns, setCampaigns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  
  // Data source selection state
  const [dataSource, setDataSource] = useState(''); // 'rb_automatic', 'product_selection', or 'file_upload'
  const [productData, setProductData] = useState([]);
  const [availableProducts, setAvailableProducts] = useState([]);
  const [selectedProducts, setSelectedProducts] = useState([]);
  
  // File upload state
  const [uploadedFile, setUploadedFile] = useState(null);
  const [fileUploadResult, setFileUploadResult] = useState(null);
  const [selectedIinColumn, setSelectedIinColumn] = useState('');
  const [supportedFormats, setSupportedFormats] = useState([]);
  
  // Campaign creation state
  const [campaignForm, setCampaignForm] = useState({
    campaign_type: 'RB1',
    metadata: {
      campaign_name: '',
      campaign_desc: '',
      stream: 'market',
      sub_stream: '',
      target_action: '',
      channel: 'Push',
      campaign_type: 'promotion',
      campaign_text: '',
      campaign_text_kz: '',
      short_desc: '',
      date_start: '',
      date_end: '',
      out_date: ''
    },
    user_iins: [],
    filter_config: {
      blacklist_tables: [],
      devices: [],
      push_streams: [],
      mau_only: false,
      products: [],
      min_age: null,
      max_age: null,
      gender: null,
      filials: [],
      local_control_streams: [],
      local_target_streams: [],
      rb3_control_streams: [],
      rb3_target_streams: [],
      previous_campaigns: [],
      cleanup_date: null,
      phone_required: false,
      info_columns: ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'],
      sum_columns: [],
      min_sum: null
    }
  });
  
  // Available options for dropdowns
  const [availableDatasets, setAvailableDatasets] = useState({
    blacklist: [],
    products: [],
    push: []
  });
  
  // Predefined filter options (from original market.py)
  const predefinedBlacklists = [
    'ACRM_DW.RB_BLACK_LIST@ACRM',
    'dssb_de.dim_clients_black_list', 
    'SPSS_USER_DRACRM.HALYK_JOB@SPSS_LNK',
    'SPSS_USER_DRACRM.BLOGGERS@SPSS_LNK',
    'dssb_app.not_recommend_credits',
    'DSSB_OCDS.mb11_global_control',
    'BL_No_worker',
    'dssb_app.abc_nbo_only',
    'dssb_app.abc_ptb_models',
    'dssb_app.abc_nbo_and_market'
  ];
  
  const pushStreams = [
    'Переводы', 'Платежи', 'Kino.kz', 'Безопасность', 'Маркет',
    'OTP/3ds/пароли', 'Инвестиции', 'Кредиты', 'Предложения Банка',
    'Депозиты', 'Onlinebank', 'Halyk страхование', 'Homebank',
    'Бонусы', 'Госуслуги', 'Operation failed HalykTravel',
    'Общий пуш', 'Oперации по счетам'
  ];
  
  const localStreams = [
    'market', 'general', 'travel', 'govtech', 'credit',
    'insurance', 'deposit', 'kino', 'transactions', 'hm'
  ];
  
  const rb3Streams = [
    'бзк', 'бизнес и налоги', 'concert', 'недвижимость', 'transfers',
    'все', 'aqyl', 'общая', 'транспорт', 'bnpl', 'card', 'realtime',
    'theatr', 'здоровья', 'плашеты', 'cards', 'нотариус и цон',
    'льготы, пособие, пенсии', 'halykapp', 'автокредит', 'ипотека',
    'kino', 'цифровые документы'
  ];
  
  const filialList = [
    'Туркестанский ОФ', 'Павлодарский ОФ', 'Северо-Казахстанский ОФ',
    'РФ Семей', 'Акмолинский ОФ', 'Шымкентский городской филиал',
    'Астанинский городской филиал', 'Жанаозенский РФ', 'Алматинский ОФ',
    'Жезказганский РФ', 'не заполнено', 'Экибастузский РФ', 'Костанайский ОФ',
    'Темиртауский РФ', 'Байконырский РФ', 'Жамбылский ОФ', 'Карагандинский ОФ',
    'АО Народный Банк Республики Казахстан', 'Шымкентский региональный филиал',
    'Актюбинский ОФ', 'Алматинский областной филиал г.Конаев', 'ОФ «Абай»',
    'Западно-Казахстанский ОФ', 'ОФ «Ұлытау»', 'Балхашский РФ',
    'Талдыкорганский ОФ', 'ОФ «Жетісу»', 'Атырауский ОФ', 'Мангистауский ОФ',
    'Астанинский РФ', 'Алматинский ГФ', 'Восточно-Казахстанский ОФ',
    'Кызылординский ОФ', 'Головной Банк', 'Семипалатинский РФ'
  ];
  
  // Generated codes
  const [generatedCodes, setGeneratedCodes] = useState({
    rb1_code: '',
    rb3_code: '',
    xls_code: ''
  });

  // Load data on component mount
  useEffect(() => {
    loadCampaigns();
    loadParquetDatasets();
    loadSupportedFormats();
  }, []);

  // Load existing campaigns
  const loadCampaigns = async () => {
    try {
      setLoading(true);
      const response = await campaignAPI.getCampaigns();
      setCampaigns(response.data.campaigns);
    } catch (err) {
      setError('Не удалось загрузить кампании: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Load parquet datasets for filtering options
  const loadParquetDatasets = async () => {
    try {
      const response = await parquetAPI.getDatasets();
      const datasets = response.data.datasets;
      
      // Categorize datasets
      const blacklistDatasets = Object.entries(datasets)
        .filter(([name, info]) => info.category === 'blacklist')
        .map(([name]) => name);
      
      const productDatasets = Object.entries(datasets)
        .filter(([name, info]) => info.category === 'products')
        .map(([name]) => name);

      const pushDatasets = Object.entries(datasets)
        .filter(([name, info]) => info.category === 'push')
        .map(([name]) => name);
      
      setAvailableDatasets({
        blacklist: blacklistDatasets,
        products: productDatasets,
        push: pushDatasets
      });
    } catch (err) {
      console.warn('Could not load parquet datasets:', err);
    }
  };

  // Load product data for product selection data source
  const loadProductData = async () => {
    try {
      setLoading(true);
      const response = await parquetAPI.filterData('final.parquet', {});
      const data = response.data.data;
      
      setProductData(data);
      
      // Extract unique products from sku_level1 column
      const products = [...new Set(data.map(row => row.sku_level1).filter(Boolean))].sort();
      setAvailableProducts(products);
      
      setSuccess(`Загружено ${data.length} записей с продуктами. Доступно ${products.length} уникальных продуктов.`);
    } catch (err) {
      setError('Не удалось загрузить продуктовые данные: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle product selection and filtering
  const handleProductSelection = (products) => {
    setSelectedProducts(products);
    
    if (products.length > 0) {
      // Filter product data by selected products
      const filteredData = productData.filter(row => 
        products.includes(row.sku_level1)
      );
      
      // Extract IINs from filtered product data
      const iins = filteredData.map(row => row.IIN).filter(Boolean);
      
      setCampaignForm(prev => ({
        ...prev,
        user_iins: iins
      }));
      
      setSuccess(`Выбрано ${products.length} продуктов. Найдено ${iins.length} клиентов.`);
    } else {
      setCampaignForm(prev => ({
        ...prev,
        user_iins: []
      }));
    }
  };

  // Generate campaign codes
  const generateRB1Code = async () => {
    try {
      const response = await campaignAPI.getNextRB1Code();
      setGeneratedCodes(prev => ({
        ...prev,
        rb1_code: response.data.campaign_code
      }));
      setCampaignForm(prev => ({
        ...prev,
        metadata: {
          ...prev.metadata,
          campaign_code: response.data.campaign_code
        }
      }));
      setSuccess('RB1 код сгенерирован: ' + response.data.campaign_code);
    } catch (err) {
      setError('Не удалось сгенерировать RB1 код: ' + (err.response?.data?.detail || err.message));
    }
  };

  const generateRB3Codes = async () => {
    try {
      const response = await campaignAPI.getNextRB3Codes();
      setGeneratedCodes(prev => ({
        ...prev,
        rb3_code: response.data.campaign_code,
        xls_code: response.data.xls_ow_id
      }));
      setCampaignForm(prev => ({
        ...prev,
        metadata: {
          ...prev.metadata,
          campaign_code: response.data.campaign_code,
          xls_ow_id: response.data.xls_ow_id
        }
      }));
      setSuccess(`RB3 коды сгенерированы: ${response.data.campaign_code}, ${response.data.xls_ow_id}`);
    } catch (err) {
      setError('Не удалось сгенерировать RB3 коды: ' + (err.response?.data?.detail || err.message));
    }
  };

  // File upload functions
  const loadSupportedFormats = async () => {
    try {
      const response = await fileAPI.getSupportedFormats();
      setSupportedFormats(response.data.supported_formats);
    } catch (err) {
      console.warn('Could not load supported formats:', err);
    }
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      const response = await fileAPI.uploadFile(file);
      const result = response.data;

      setUploadedFile(file);
      setFileUploadResult(result);
      setSelectedIinColumn(result.iin_column || '');

      if (result.validation_errors && result.validation_errors.length > 0) {
        setError(`Файл загружен с предупреждениями: ${result.validation_errors.slice(0, 3).join('; ')}`);
      } else {
        setSuccess(`Файл загружен успешно! Найдено ${result.iins_extracted} IIN в ${result.rows_processed} строках.`);
      }

      // Set IINs to campaign form
      if (result.iin_column) {
        // We'll get the IINs from the processing step
        setSuccess(prev => prev + ` Колонка IIN: ${result.iin_column}`);
      }

    } catch (err) {
      setError('Ошибка загрузки файла: ' + (err.response?.data?.detail || err.message));
      setUploadedFile(null);
      setFileUploadResult(null);
    } finally {
      setLoading(false);
    }
  };

  const processUploadedFile = async () => {
    if (!fileUploadResult || !selectedIinColumn) {
      setError('Пожалуйста, выберите колонку IIN');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fileAPI.processFile(
        fileUploadResult.filename,
        selectedIinColumn,
        campaignForm.filter_config
      );

      const result = response.data;

      setCampaignForm(prev => ({
        ...prev,
        user_iins: result.iins
      }));

      setSuccess(`Файл обработан! Получено ${result.filtered_count} IIN из ${result.original_count} исходных записей.`);

    } catch (err) {
      setError('Ошибка обработки файла: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Handle form input changes
  const handleMetadataChange = (field, value) => {
    setCampaignForm(prev => ({
      ...prev,
      metadata: {
        ...prev.metadata,
        [field]: value
      }
    }));
  };

  const handleFilterChange = (field, value) => {
    setCampaignForm(prev => ({
      ...prev,
      filter_config: {
        ...prev.filter_config,
        [field]: value
      }
    }));
  };

  // Handle IIN list input
  const handleIINsChange = (value) => {
    // Split by commas, newlines, or spaces and filter out empty strings
    const iins = value.split(/[,\n\s]+/).filter(iin => iin.trim().length > 0);
    setCampaignForm(prev => ({
      ...prev,
      user_iins: iins
    }));
  };

  // Create campaign
  const createCampaign = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Validate required fields
      if (!campaignForm.metadata.campaign_name) {
        setError('Название кампании обязательно');
        return;
      }
      
      // Validate data source specific requirements
      if (dataSource === 'file_upload' && (!fileUploadResult || !selectedIinColumn)) {
        setError('Необходимо загрузить и обработать файл с клиентами');
        return;
      }
      
      if (campaignForm.user_iins.length === 0) {
        setError('Необходимо указать хотя бы один IIN');
        return;
      }

      const response = await campaignAPI.createCampaign(campaignForm);
      
      if (response.data.success) {
        setSuccess(`Кампания ${response.data.campaign_code} успешно создана!`);
        setActiveTab('list');
        loadCampaigns();
        
        // Reset form
        setCampaignForm({
          campaign_type: 'RB1',
          metadata: {
            campaign_name: '',
            campaign_desc: '',
            stream: 'market',
            sub_stream: '',
            target_action: '',
            channel: 'Push',
            campaign_type: 'promotion',
            campaign_text: '',
            campaign_text_kz: '',
            short_desc: '',
            date_start: '',
            date_end: '',
            out_date: ''
          },
          user_iins: [],
          filter_config: {
            blacklist_tables: [],
            devices: [],
            push_streams: [],
            mau_only: false,
            products: [],
            min_age: null,
            max_age: null,
            gender: null,
            filials: [],
            local_control_streams: [],
            local_target_streams: [],
            rb3_control_streams: [],
            rb3_target_streams: [],
            previous_campaigns: [],
            cleanup_date: null,
            phone_required: false,
            info_columns: ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'],
            sum_columns: [],
            min_sum: null
          }
        });
        
        // Reset file upload state
        setDataSource('');
        setUploadedFile(null);
        setFileUploadResult(null);
        setSelectedIinColumn('');
      } else {
        setError('Ошибка создания кампании: ' + response.data.message);
      }
    } catch (err) {
      setError('Не удалось создать кампанию: ' + (err.response?.data?.detail || err.message));
    } finally {
      setLoading(false);
    }
  };

  // Delete campaign
  const deleteCampaign = async (campaignCode) => {
    if (!window.confirm(`Вы уверены, что хотите удалить кампанию ${campaignCode}?`)) {
      return;
    }

    try {
      await campaignAPI.deleteCampaign(campaignCode);
      setSuccess(`Кампания ${campaignCode} удалена`);
      loadCampaigns();
    } catch (err) {
      setError('Не удалось удалить кампанию: ' + (err.response?.data?.detail || err.message));
    }
  };

  // Clear messages
  const clearMessages = () => {
    setError(null);
    setSuccess(null);
  };

  return (
    <div className="campaign-manager" style={{padding: '20px'}}>
      <div className="page-header">
        <h1>🎯 Управление кампаниями</h1>
        <p>Создание и управление RB1/RB3 кампаниями с интеграцией фильтрации данных</p>
      </div>

      {/* Messages */}
      {error && (
        <div style={{padding: '10px', backgroundColor: '#ffe6e6', border: '1px solid #ff4444', borderRadius: '4px', marginBottom: '10px'}}>
          ❌ {error}
        </div>
      )}
      
      {success && (
        <div style={{padding: '10px', backgroundColor: '#e6ffe6', border: '1px solid #44ff44', borderRadius: '4px', marginBottom: '10px'}}>
          ✅ {success}
        </div>
      )}

      {/* Tab Navigation */}
      <div style={{borderBottom: '1px solid #ddd', marginBottom: '20px'}}>
        <button 
          style={{
            padding: '10px 20px',
            border: 'none',
            backgroundColor: activeTab === 'list' ? '#007bff' : 'transparent',
            color: activeTab === 'list' ? 'white' : 'black',
            cursor: 'pointer'
          }}
          onClick={() => setActiveTab('list')}
        >
          📋 Список кампаний
        </button>
        <button 
          style={{
            padding: '10px 20px',
            border: 'none',
            backgroundColor: activeTab === 'create' ? '#007bff' : 'transparent',
            color: activeTab === 'create' ? 'white' : 'black',
            cursor: 'pointer'
          }}
          onClick={() => setActiveTab('create')}
        >
          ➕ Создать кампанию
        </button>
        <button 
          style={{
            padding: '10px 20px',
            border: 'none',
            backgroundColor: activeTab === 'filter' ? '#007bff' : 'transparent',
            color: activeTab === 'filter' ? 'white' : 'black',
            cursor: 'pointer'
          }}
          onClick={() => setActiveTab('filter')}
        >
          🔍 Тест фильтров
        </button>
        <button 
          style={{
            padding: '10px 20px',
            border: 'none',
            backgroundColor: activeTab === 'codes' ? '#007bff' : 'transparent',
            color: activeTab === 'codes' ? 'white' : 'black',
            cursor: 'pointer'
          }}
          onClick={() => setActiveTab('codes')}
        >
          🔢 Генерация кодов
        </button>
        <button 
          style={{
            padding: '10px 20px',
            border: 'none',
            backgroundColor: activeTab === 'tools' ? '#007bff' : 'transparent',
            color: activeTab === 'tools' ? 'white' : 'black',
            cursor: 'pointer'
          }}
          onClick={() => setActiveTab('tools')}
        >
          🛠️ Инструменты
        </button>
      </div>

      {/* Tab Content */}
      {activeTab === 'list' && (
        <div>
          <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px'}}>
            <h2>Существующие кампании</h2>
            <button onClick={loadCampaigns} disabled={loading} style={{padding: '8px 16px'}}>
              🔄 Обновить
            </button>
          </div>
          
          {loading ? (
            <div>Загрузка кампаний...</div>
          ) : campaigns.length === 0 ? (
            <div>
              <p>Кампании не найдены</p>
              <button onClick={() => setActiveTab('create')}>Создать первую кампанию</button>
            </div>
          ) : (
            <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '20px'}}>
              {campaigns.map((campaign) => (
                <div key={campaign.campaign_code} style={{
                  border: '1px solid #ddd',
                  borderRadius: '8px',
                  padding: '16px',
                  backgroundColor: '#f9f9f9'
                }}>
                  <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '10px'}}>
                    <h3 style={{margin: 0}}>{campaign.campaign_name}</h3>
                    <span style={{
                      padding: '4px 8px',
                      backgroundColor: campaign.campaign_type === 'RB1' ? '#007bff' : '#28a745',
                      color: 'white',
                      borderRadius: '4px',
                      fontSize: '12px'
                    }}>
                      {campaign.campaign_type}
                    </span>
                  </div>
                  <div style={{fontSize: '14px', lineHeight: '1.5'}}>
                    <p><strong>Код:</strong> {campaign.campaign_code}</p>
                    <p><strong>Поток:</strong> {campaign.stream}</p>
                    <p><strong>Канал:</strong> {campaign.channel}</p>
                    <p><strong>Период:</strong> {campaign.date_start} - {campaign.date_end}</p>
                  </div>
                  <button 
                    style={{
                      backgroundColor: '#dc3545',
                      color: 'white',
                      border: 'none',
                      padding: '8px 12px',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      marginTop: '10px'
                    }}
                    onClick={() => deleteCampaign(campaign.campaign_code)}
                  >
                    🗑️ Удалить
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {activeTab === 'create' && (
        <div>
          <h2>Создание новой кампании</h2>
          
          {/* Data Source Selection - First Step */}
          <div style={{
            marginBottom: '30px', 
            border: '2px solid #007bff', 
            borderRadius: '8px', 
            padding: '20px', 
            backgroundColor: '#f8f9fa'
          }}>
            <h3 style={{color: '#007bff', marginTop: 0}}>📊 1. Выберите источник данных для UserID</h3>
            <p style={{color: '#6c757d', marginBottom: '20px'}}>
              Выберите способ получения начальной выборки клиентов для кампании
            </p>
            
            <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '20px'}}>
              <label style={{
                display: 'block',
                padding: '20px',
                border: dataSource === 'rb_automatic' ? '2px solid #007bff' : '1px solid #ddd',
                borderRadius: '8px',
                backgroundColor: dataSource === 'rb_automatic' ? '#e3f2fd' : 'white',
                cursor: 'pointer',
                transition: 'all 0.3s ease'
              }}>
                <input 
                  type="radio" 
                  value="rb_automatic" 
                  checked={dataSource === 'rb_automatic'}
                  onChange={(e) => setDataSource(e.target.value)}
                  style={{marginRight: '10px'}}
                />
                <div>
                  <h4 style={{margin: '0 0 10px 0', color: '#007bff'}}>🚀 РБ Автоматический запуск</h4>
                  <p style={{margin: '0', fontSize: '14px', color: '#666'}}>
                    Стандартный подход: начать с основной базы rb_feature_store и применить фильтры.
                    Подходит для большинства маркетинговых кампаний.
                  </p>
                  <ul style={{margin: '10px 0 0 20px', fontSize: '13px', color: '#666'}}>
                    <li>Доступ ко всем клиентам банка</li>
                    <li>Множественные фильтры по демографии</li>
                    <li>Исключение стоп-листов</li>
                  </ul>
                </div>
              </label>
              
              <label style={{
                display: 'block',
                padding: '20px',
                border: dataSource === 'product_selection' ? '2px solid #28a745' : '1px solid #ddd',
                borderRadius: '8px',
                backgroundColor: dataSource === 'product_selection' ? '#e8f5e8' : 'white',
                cursor: 'pointer',
                transition: 'all 0.3s ease'
              }}>
                <input 
                  type="radio" 
                  value="product_selection" 
                  checked={dataSource === 'product_selection'}
                  onChange={(e) => setDataSource(e.target.value)}
                  style={{marginRight: '10px'}}
                />
                <div>
                  <h4 style={{margin: '0 0 10px 0', color: '#28a745'}}>🛍️ Продуктовая выборка</h4>
                  <p style={{margin: '0', fontSize: '14px', color: '#666'}}>
                    Таргетинг по продуктам: начать с клиентов, которые имеют связь с определенными продуктами.
                    Идеально для продуктовых кампаний.
                  </p>
                  <ul style={{margin: '10px 0 0 20px', fontSize: '13px', color: '#666'}}>
                    <li>Фокус на продуктовых отношениях</li>
                    <li>Точный таргетинг по интересам</li>
                    <li>Высокая релевантность предложений</li>
                  </ul>
                </div>
              </label>

              <label style={{
                display: 'block',
                padding: '20px',
                border: dataSource === 'file_upload' ? '2px solid #dc3545' : '1px solid #ddd',
                borderRadius: '8px',
                backgroundColor: dataSource === 'file_upload' ? '#fdf2f2' : 'white',
                cursor: 'pointer',
                transition: 'all 0.3s ease'
              }}>
                <input 
                  type="radio" 
                  value="file_upload" 
                  checked={dataSource === 'file_upload'}
                  onChange={(e) => setDataSource(e.target.value)}
                  style={{marginRight: '10px'}}
                />
                <div>
                  <h4 style={{margin: '0 0 10px 0', color: '#dc3545'}}>📁 Загрузка файла</h4>
                  <p style={{margin: '0', fontSize: '14px', color: '#666'}}>
                    Загрузите готовый список клиентов из Excel, CSV или Parquet файла.
                    Подходит для готовых выборок и внешних списков.
                  </p>
                  <ul style={{margin: '10px 0 0 20px', fontSize: '13px', color: '#666'}}>
                    <li>Поддержка Excel (.xlsx, .xls)</li>
                    <li>CSV файлы с различными кодировками</li>
                    <li>Parquet файлы для больших данных</li>
                    <li>Автоматическое определение IIN колонки</li>
                  </ul>
                </div>
              </label>
            </div>
            
            {!dataSource && (
              <div style={{
                marginTop: '15px',
                padding: '10px',
                backgroundColor: '#fff3cd',
                border: '1px solid #ffeaa7',
                borderRadius: '4px',
                color: '#856404'
              }}>
                ⚠️ Пожалуйста, выберите источник данных для продолжения
              </div>
            )}
          </div>

          {/* Data Source Specific Configuration */}
          {dataSource === 'product_selection' && (
            <div style={{
              marginBottom: '30px', 
              border: '1px solid #28a745', 
              borderRadius: '8px', 
              padding: '20px', 
              backgroundColor: '#f8fff8'
            }}>
              <h3 style={{color: '#28a745', marginTop: 0}}>🛍️ Настройка продуктовой выборки</h3>
              
              {productData.length === 0 ? (
                <div>
                  <p style={{color: '#666', marginBottom: '15px'}}>
                    Сначала загрузите продуктовые данные из final.parquet
                  </p>
                  <button 
                    onClick={loadProductData}
                    disabled={loading}
                    style={{
                      padding: '12px 24px',
                      backgroundColor: '#28a745',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      fontSize: '16px'
                    }}
                  >
                    {loading ? '⏳ Загружаем...' : '📥 Загрузить продуктовые данные'}
                  </button>
                </div>
              ) : (
                <div>
                  <p style={{color: '#666', marginBottom: '15px'}}>
                    Загружено {productData.length} записей. Выберите продукты для таргетинга:
                  </p>
                  
                  <div style={{marginBottom: '15px'}}>
                    <label style={{display: 'block', marginBottom: '8px', fontWeight: '500'}}>
                      Продукты для кампании (выберите один или несколько):
                    </label>
                    <select
                      multiple
                      value={selectedProducts}
                      onChange={(e) => {
                        const selected = Array.from(e.target.selectedOptions, option => option.value);
                        handleProductSelection(selected);
                      }}
                      style={{
                        width: '100%',
                        minHeight: '120px',
                        padding: '8px',
                        border: '1px solid #ddd',
                        borderRadius: '4px'
                      }}
                    >
                      {availableProducts.map(product => (
                        <option key={product} value={product}>
                          {product}
                        </option>
                      ))}
                    </select>
                    <small style={{color: '#666'}}>
                      Удерживайте Ctrl (Cmd на Mac) для выбора нескольких продуктов
                    </small>
                  </div>
                  
                  {selectedProducts.length > 0 && (
                    <div style={{
                      padding: '10px',
                      backgroundColor: '#d4edda',
                      border: '1px solid #c3e6cb',
                      borderRadius: '4px',
                      marginBottom: '10px'
                    }}>
                      ✅ Выбранные продукты: <strong>{selectedProducts.join(', ')}</strong>
                      <br />
                      👥 Найдено клиентов: <strong>{campaignForm.user_iins.length}</strong>
                    </div>
                  )}
                  
                  <button 
                    onClick={loadProductData}
                    disabled={loading}
                    style={{
                      padding: '8px 16px',
                      backgroundColor: '#6c757d',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer'
                    }}
                  >
                    🔄 Перезагрузить данные
                  </button>
                </div>
              )}
            </div>
          )}

          {dataSource === 'rb_automatic' && (
            <div style={{
              marginBottom: '30px', 
              border: '1px solid #007bff', 
              borderRadius: '8px', 
              padding: '20px', 
              backgroundColor: '#f8f9ff'
            }}>
              <h3 style={{color: '#007bff', marginTop: 0}}>🚀 Настройка автоматического запуска</h3>
              <p style={{color: '#666', marginBottom: '15px'}}>
                При автоматическом запуске базовая выборка будет загружена из rb_feature_store.
                Вы сможете применить фильтры на следующих шагах.
              </p>
              
              {/* Information Columns Selection */}
              <div style={{marginBottom: '20px'}}>
                <h4 style={{color: '#495057', marginBottom: '10px'}}>📋 Информационные колонки</h4>
                <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '8px'}}>
                  {['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'].map(column => (
                    <label key={column} style={{
                      display: 'flex',
                      alignItems: 'center',
                      padding: '8px',
                      backgroundColor: 'white',
                      border: '1px solid #ddd',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      fontSize: '13px'
                    }}>
                      <input
                        type="checkbox"
                        checked={campaignForm.filter_config.info_columns?.includes(column) || ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'].includes(column)}
                        onChange={(e) => {
                          const current = campaignForm.filter_config.info_columns || ['SNAPSHOT_DATE', 'IIN', 'P_SID', 'PUBLIC_ID', 'IS_MAU'];
                          const updated = e.target.checked 
                            ? [...current, column]
                            : current.filter(c => c !== column);
                          handleFilterChange('info_columns', updated);
                        }}
                        style={{marginRight: '8px'}}
                      />
                      {column}
                    </label>
                  ))}
                </div>
                <small style={{color: '#6c757d'}}>
                  Выберите информационные колонки для загрузки из rb_feature_store
                </small>
              </div>

              {/* Sum Columns Selection */}
              <div style={{marginBottom: '20px'}}>
                <h4 style={{color: '#495057', marginBottom: '10px'}}>💰 Суммируемые колонки (для фильтрации по активности)</h4>
                <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '8px'}}>
                  {[
                    'CARD_PURCHASE_FREQUENCY', 'CARD_PURCHASE_SUM', 'LOAN_CNT', 'SAFE_CNT', 
                    'ACCOUNT_CNT', 'CREDIT_CARD_CNT', 'DEBIT_CARD_CNT', 'BEZNAL_TRANSACTION_CNT', 
                    'BEZNAL_AMOUNT', 'OBNAL_TRANSACTION_CNT'
                  ].map(column => (
                    <label key={column} style={{
                      display: 'flex',
                      alignItems: 'center',
                      padding: '8px',
                      backgroundColor: 'white',
                      border: '1px solid #ddd',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      fontSize: '13px'
                    }}>
                      <input
                        type="checkbox"
                        checked={campaignForm.filter_config.sum_columns?.includes(column) || false}
                        onChange={(e) => {
                          const current = campaignForm.filter_config.sum_columns || [];
                          const updated = e.target.checked 
                            ? [...current, column]
                            : current.filter(c => c !== column);
                          handleFilterChange('sum_columns', updated);
                        }}
                        style={{marginRight: '8px'}}
                      />
                      {column}
                    </label>
                  ))}
                </div>
                <small style={{color: '#6c757d'}}>
                  Выбранные колонки будут суммированы в поле Column_sum для фильтрации по общей активности. 
                  Выбрано: {campaignForm.filter_config.sum_columns?.length || 0}
                </small>
              </div>

              {/* Sum Filter */}
              {campaignForm.filter_config.sum_columns?.length > 0 && (
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{color: '#495057', marginBottom: '10px'}}>🔢 Фильтр по сумме активности</h4>
                  <div style={{display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '15px', alignItems: 'end'}}>
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>
                        Минимальная сумма активности
                      </label>
                      <input
                        type="number"
                        min="0"
                        step="1"
                        value={campaignForm.filter_config.min_sum || ''}
                        onChange={(e) => handleFilterChange('min_sum', e.target.value ? parseFloat(e.target.value) : null)}
                        placeholder="0"
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      />
                    </div>
                    <div style={{
                      padding: '10px',
                      backgroundColor: '#e3f2fd',
                      border: '1px solid #2196f3',
                      borderRadius: '4px',
                      fontSize: '14px'
                    }}>
                      <strong>Активные колонки:</strong> {campaignForm.filter_config.sum_columns.join(', ')}
                      <br />
                      <small style={{color: '#1976d2'}}>
                        Клиенты с Column_sum ≥ {campaignForm.filter_config.min_sum || 0} будут включены в кампанию
                      </small>
                    </div>
                  </div>
                </div>
              )}

              <div style={{
                padding: '10px',
                backgroundColor: '#cce5ff',
                border: '1px solid #99d6ff',
                borderRadius: '4px'
              }}>
                ℹ️ Базовая выборка будет сформирована автоматически на основе выбранных колонок и фильтров
              </div>
            </div>
          )}

          {/* File Upload Configuration */}
          {dataSource === 'file_upload' && (
            <div style={{
              marginBottom: '30px', 
              border: '1px solid #dc3545', 
              borderRadius: '8px', 
              padding: '20px', 
              backgroundColor: '#fff8f8'
            }}>
              <h3 style={{color: '#dc3545', marginTop: 0}}>📁 Загрузка файла с клиентами</h3>
              
              {!fileUploadResult ? (
                <div>
                  <p style={{color: '#666', marginBottom: '15px'}}>
                    Загрузите файл со списком клиентов. Поддерживаются форматы: Excel (.xlsx, .xls), CSV, Parquet
                  </p>
                  
                  {/* File Upload Input */}
                  <div style={{marginBottom: '20px'}}>
                    <label style={{
                      display: 'block',
                      padding: '40px 20px',
                      border: '2px dashed #dc3545',
                      borderRadius: '8px',
                      textAlign: 'center',
                      cursor: 'pointer',
                      backgroundColor: '#ffffff',
                      transition: 'all 0.3s ease'
                    }}>
                      <input
                        type="file"
                        accept=".xlsx,.xls,.csv,.parquet"
                        onChange={handleFileUpload}
                        style={{display: 'none'}}
                        disabled={loading}
                      />
                      <div style={{fontSize: '48px', marginBottom: '15px'}}>📄</div>
                      <div style={{fontSize: '18px', fontWeight: '500', marginBottom: '10px', color: '#dc3545'}}>
                        {loading ? '⏳ Загружаем...' : 'Выберите файл или перетащите сюда'}
                      </div>
                      <div style={{fontSize: '14px', color: '#666'}}>
                        Поддерживаемые форматы: .xlsx, .xls, .csv, .parquet (макс. 50MB)
                      </div>
                    </label>
                  </div>

                  {/* Supported Formats Info */}
                  {supportedFormats.length > 0 && (
                    <div style={{
                      padding: '15px',
                      backgroundColor: '#f8f9fa',
                      border: '1px solid #dee2e6',
                      borderRadius: '4px',
                      marginBottom: '20px'
                    }}>
                      <h5 style={{margin: '0 0 10px 0', color: '#495057'}}>Поддерживаемые форматы:</h5>
                      <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '10px'}}>
                        {supportedFormats.map((format, index) => (
                          <div key={index} style={{
                            padding: '8px 12px',
                            backgroundColor: 'white',
                            border: '1px solid #ddd',
                            borderRadius: '4px',
                            fontSize: '13px'
                          }}>
                            <strong>{format.extension}</strong> - {format.description}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div>
                  <div style={{
                    padding: '15px',
                    backgroundColor: '#d4edda',
                    border: '1px solid #c3e6cb',
                    borderRadius: '4px',
                    marginBottom: '20px'
                  }}>
                    <h5 style={{margin: '0 0 10px 0', color: '#155724'}}>
                      ✅ Файл загружен: {uploadedFile?.name}
                    </h5>
                    <p style={{margin: '0', fontSize: '14px', color: '#155724'}}>
                      Обработано {fileUploadResult.rows_processed} строк, найдено {fileUploadResult.iins_extracted} IIN
                    </p>
                  </div>

                  {/* IIN Column Selection */}
                  <div style={{marginBottom: '20px'}}>
                    <h4 style={{color: '#495057', marginBottom: '10px'}}>🎯 Выбор колонки IIN</h4>
                    <div style={{display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '15px', alignItems: 'start'}}>
                      <div>
                        <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>
                          Колонка с IIN
                        </label>
                        <select
                          value={selectedIinColumn}
                          onChange={(e) => setSelectedIinColumn(e.target.value)}
                          style={{
                            width: '100%',
                            padding: '8px',
                            border: '1px solid #ddd',
                            borderRadius: '4px'
                          }}
                        >
                          <option value="">Выберите колонку...</option>
                          {fileUploadResult.columns_detected.map(column => (
                            <option key={column} value={column}>{column}</option>
                          ))}
                        </select>
                      </div>
                      
                      <div style={{
                        padding: '15px',
                        backgroundColor: '#e3f2fd',
                        border: '1px solid #2196f3',
                        borderRadius: '4px'
                      }}>
                        <h6 style={{margin: '0 0 10px 0', color: '#1976d2'}}>Образец данных:</h6>
                        <div style={{fontSize: '12px', fontFamily: 'monospace'}}>
                          {fileUploadResult.sample_data.slice(0, 3).map((row, idx) => (
                            <div key={idx} style={{marginBottom: '5px'}}>
                              {Object.entries(row).slice(0, 3).map(([key, value]) => (
                                <span key={key} style={{marginRight: '15px'}}>
                                  <strong>{key}:</strong> {String(value).substring(0, 20)}
                                </span>
                              ))}
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Process Button */}
                  <div style={{marginBottom: '20px'}}>
                    <button 
                      onClick={processUploadedFile}
                      disabled={loading || !selectedIinColumn}
                      style={{
                        padding: '12px 24px',
                        backgroundColor: selectedIinColumn ? '#dc3545' : '#6c757d',
                        color: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: selectedIinColumn && !loading ? 'pointer' : 'not-allowed',
                        fontSize: '16px',
                        marginRight: '15px'
                      }}
                    >
                      {loading ? '⏳ Обрабатываем...' : '🔄 Обработать файл с фильтрами'}
                    </button>
                    
                    <button 
                      onClick={() => {
                        setFileUploadResult(null);
                        setUploadedFile(null);
                        setSelectedIinColumn('');
                      }}
                      style={{
                        padding: '12px 24px',
                        backgroundColor: '#6c757d',
                        color: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontSize: '16px'
                      }}
                    >
                      🗑️ Загрузить другой файл
                    </button>
                  </div>

                  {/* File Processing Info */}
                  <div style={{
                    padding: '10px',
                    backgroundColor: '#fff3cd',
                    border: '1px solid #ffeaa7',
                    borderRadius: '4px'
                  }}>
                    ℹ️ После обработки файла будут применены выбранные фильтры. 
                    Если фильтры не нужны, нажмите "Обработать файл с фильтрами" без настройки фильтров.
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Continue only if data source is selected */}
          {dataSource && (
            <>
              {/* Campaign Type */}
              <div style={{marginBottom: '20px'}}>
                <h3>📋 2. Тип кампании</h3>
                <label style={{marginRight: '20px'}}>
                  <input 
                    type="radio" 
                    value="RB1" 
                    checked={campaignForm.campaign_type === 'RB1'}
                    onChange={(e) => setCampaignForm(prev => ({...prev, campaign_type: e.target.value}))}
                    style={{marginRight: '5px'}}
                  />
                  RB1 - Стандартная кампания
                </label>
                <label>
                  <input 
                    type="radio" 
                    value="RB3" 
                    checked={campaignForm.campaign_type === 'RB3'}
                    onChange={(e) => setCampaignForm(prev => ({...prev, campaign_type: e.target.value}))}
                    style={{marginRight: '5px'}}
                  />
                  RB3 - Бонусная кампания
                </label>
              </div>

              {/* Basic Information */}
              <div style={{marginBottom: '20px'}}>
                <h3>Основная информация</h3>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px'}}>
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Название кампании *</label>
                    <input
                      type="text"
                      value={campaignForm.metadata.campaign_name}
                      onChange={(e) => handleMetadataChange('campaign_name', e.target.value)}
                      placeholder="Название кампании"
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Краткое описание *</label>
                    <input
                      type="text"
                      value={campaignForm.metadata.short_desc}
                      onChange={(e) => handleMetadataChange('short_desc', e.target.value)}
                      placeholder="Краткое описание"
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                </div>
                
                <div style={{marginTop: '15px'}}>
                  <label style={{display: 'block', marginBottom: '5px'}}>Описание кампании</label>
                  <textarea
                    value={campaignForm.metadata.campaign_desc}
                    onChange={(e) => handleMetadataChange('campaign_desc', e.target.value)}
                    placeholder="Подробное описание"
                    style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px', minHeight: '80px'}}
                  />
                </div>
              </div>

              {/* Targeting */}
              <div style={{marginBottom: '20px'}}>
                <h3>Настройки таргетинга</h3>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '15px'}}>
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Поток</label>
                    <select
                      value={campaignForm.metadata.stream}
                      onChange={(e) => handleMetadataChange('stream', e.target.value)}
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    >
                      <option value="market">market</option>
                      <option value="general">general</option>
                      <option value="credit">credit</option>
                    </select>
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Канал</label>
                    <select
                      value={campaignForm.metadata.channel}
                      onChange={(e) => handleMetadataChange('channel', e.target.value)}
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    >
                      <option value="Push">Push</option>
                      <option value="POP-UP">POP-UP</option>
                      <option value="СМС">СМС</option>
                    </select>
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Целевое действие</label>
                    <input
                      type="text"
                      value={campaignForm.metadata.target_action}
                      onChange={(e) => handleMetadataChange('target_action', e.target.value)}
                      placeholder="purchase, click, etc."
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                </div>
              </div>

              {/* Campaign Text */}
              <div style={{marginBottom: '20px'}}>
                <h3>Текст кампании</h3>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px'}}>
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Текст (RU)</label>
                    <textarea
                      value={campaignForm.metadata.campaign_text}
                      onChange={(e) => handleMetadataChange('campaign_text', e.target.value)}
                      placeholder="Текст на русском"
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px', minHeight: '80px'}}
                    />
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Текст (KZ)</label>
                    <textarea
                      value={campaignForm.metadata.campaign_text_kz}
                      onChange={(e) => handleMetadataChange('campaign_text_kz', e.target.value)}
                      placeholder="Текст на казахском"
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px', minHeight: '80px'}}
                    />
                  </div>
                </div>
              </div>

              {/* Dates */}
              <div style={{marginBottom: '20px'}}>
                <h3>Даты кампании</h3>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '15px'}}>
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Дата начала</label>
                    <input
                      type="date"
                      value={campaignForm.metadata.date_start}
                      onChange={(e) => handleMetadataChange('date_start', e.target.value)}
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Дата окончания</label>
                    <input
                      type="date"
                      value={campaignForm.metadata.date_end}
                      onChange={(e) => handleMetadataChange('date_end', e.target.value)}
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                  
                  <div>
                    <label style={{display: 'block', marginBottom: '5px'}}>Дата отправки</label>
                    <input
                      type="date"
                      value={campaignForm.metadata.out_date}
                      onChange={(e) => handleMetadataChange('out_date', e.target.value)}
                      style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                    />
                  </div>
                </div>
              </div>

              {/* User IINs */}
              <div style={{marginBottom: '20px'}}>
                <h3>Список пользователей</h3>
                <label style={{display: 'block', marginBottom: '5px'}}>IIN пользователей * (по одному на строку)</label>
                <textarea
                  value={campaignForm.user_iins.join('\n')}
                  onChange={(e) => handleIINsChange(e.target.value)}
                  placeholder="123456789012&#10;987654321098"
                  style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px', minHeight: '120px'}}
                />
                <small style={{color: '#666'}}>Указано IIN: {campaignForm.user_iins.length}</small>
              </div>

              {/* Data Filtering Section */}
              <div style={{marginBottom: '20px', border: '1px solid #ddd', borderRadius: '8px', padding: '20px', backgroundColor: '#f8f9fa'}}>
                <h3 style={{marginTop: 0, color: '#495057'}}>🔍 Фильтрация данных</h3>
                <p style={{color: '#6c757d', fontSize: '14px', marginBottom: '20px'}}>
                  Настройте фильтры для исключения нежелательных пользователей из кампании
                </p>
                
                {/* Blacklist Tables */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🚫 Стоп-листы (Blacklists)</h4>
                  <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '10px'}}>
                    {predefinedBlacklists.map(dataset => (
                      <label key={dataset} style={{
                        display: 'flex',
                        alignItems: 'center',
                        padding: '8px',
                        backgroundColor: 'white',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontSize: '13px'
                      }}>
                        <input
                          type="checkbox"
                          checked={campaignForm.filter_config.blacklist_tables.includes(dataset)}
                          onChange={(e) => {
                            const current = campaignForm.filter_config.blacklist_tables;
                            const updated = e.target.checked 
                              ? [...current, dataset]
                              : current.filter(d => d !== dataset);
                            handleFilterChange('blacklist_tables', updated);
                          }}
                          style={{marginRight: '8px'}}
                        />
                        <span title={dataset}>{dataset.split('.').pop()}</span>
                      </label>
                    ))}
                  </div>
                  <small style={{color: '#6c757d'}}>
                    Выбрано: {campaignForm.filter_config.blacklist_tables.length} из {predefinedBlacklists.length}
                  </small>
                </div>

                {/* Device Filtering */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>📱 Фильтрация по устройствам</h4>
                  <div style={{display: 'flex', gap: '15px'}}>
                    {['android', 'iOS'].map(device => (
                      <label key={device} style={{
                        display: 'flex',
                        alignItems: 'center',
                        padding: '10px 15px',
                        backgroundColor: 'white',
                        border: '1px solid #ddd',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        minWidth: '120px'
                      }}>
                        <input
                          type="checkbox"
                          checked={campaignForm.filter_config.devices.includes(device)}
                          onChange={(e) => {
                            const current = campaignForm.filter_config.devices;
                            const updated = e.target.checked 
                              ? [...current, device]
                              : current.filter(d => d !== device);
                            handleFilterChange('devices', updated);
                          }}
                          style={{marginRight: '8px'}}
                        />
                        {device === 'android' ? '🤖' : '🍎'} {device}
                      </label>
                    ))}
                  </div>
                  <small style={{color: '#6c757d'}}>
                    Выберите устройства для таргетинга. Пустой выбор = все устройства.
                  </small>
                </div>

                {/* Push Stream Filtering */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🔔 Фильтрация Push-уведомлений</h4>
                  <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '8px', maxHeight: '200px', overflowY: 'auto', border: '1px solid #ddd', padding: '10px', borderRadius: '4px', backgroundColor: 'white'}}>
                    {pushStreams.map(stream => (
                      <label key={stream} style={{
                        display: 'flex',
                        alignItems: 'center',
                        padding: '5px',
                        cursor: 'pointer',
                        fontSize: '13px'
                      }}>
                        <input
                          type="checkbox"
                          checked={campaignForm.filter_config.push_streams.includes(stream)}
                          onChange={(e) => {
                            const current = campaignForm.filter_config.push_streams;
                            const updated = e.target.checked 
                              ? [...current, stream]
                              : current.filter(s => s !== stream);
                            handleFilterChange('push_streams', updated);
                          }}
                          style={{marginRight: '6px'}}
                        />
                        {stream}
                      </label>
                    ))}
                  </div>
                  <small style={{color: '#6c757d'}}>
                    Исключить пользователей, отключивших эти потоки уведомлений. Выбрано: {campaignForm.filter_config.push_streams.length}
                  </small>
                </div>

                {/* MAU Filtering */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>👥 Фильтрация активности</h4>
                  <label style={{
                    display: 'flex',
                    alignItems: 'center',
                    padding: '12px 15px',
                    backgroundColor: 'white',
                    border: '1px solid #ddd',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    maxWidth: '300px'
                  }}>
                    <input
                      type="checkbox"
                      checked={campaignForm.filter_config.mau_only}
                      onChange={(e) => handleFilterChange('mau_only', e.target.checked)}
                      style={{marginRight: '10px'}}
                    />
                    <div>
                      <div style={{fontWeight: '500'}}>📊 Только MAU пользователи</div>
                      <small style={{color: '#6c757d'}}>Активные в течение месяца</small>
                    </div>
                  </label>
                </div>

                {/* Age Filtering */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🎂 Фильтрация по возрасту</h4>
                  <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '15px', alignItems: 'end'}}>
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>Минимальный возраст</label>
                      <input
                        type="number"
                        min="18"
                        max="100"
                        value={campaignForm.filter_config.min_age || ''}
                        onChange={(e) => handleFilterChange('min_age', e.target.value ? parseInt(e.target.value) : null)}
                        placeholder="18"
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      />
                    </div>
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>Максимальный возраст</label>
                      <input
                        type="number"
                        min="18"
                        max="100"
                        value={campaignForm.filter_config.max_age || ''}
                        onChange={(e) => handleFilterChange('max_age', e.target.value ? parseInt(e.target.value) : null)}
                        placeholder="65"
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      />
                    </div>
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>Пол</label>
                      <select
                        value={campaignForm.filter_config.gender || ''}
                        onChange={(e) => handleFilterChange('gender', e.target.value || null)}
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      >
                        <option value="">Все</option>
                        <option value="M">Мужской</option>
                        <option value="F">Женский</option>
                      </select>
                    </div>
                  </div>
                  <small style={{color: '#6c757d'}}>
                    Возраст определяется по IIN или данным клиента. Пустые поля = без ограничений.
                  </small>
                </div>

                {/* Filial Filtering */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🏢 Фильтрация по филиалам</h4>
                  <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '8px', maxHeight: '200px', overflowY: 'auto', border: '1px solid #ddd', padding: '10px', borderRadius: '4px', backgroundColor: 'white'}}>
                    {filialList.map(filial => (
                      <label key={filial} style={{
                        display: 'flex',
                        alignItems: 'center',
                        padding: '5px',
                        cursor: 'pointer',
                        fontSize: '13px'
                      }}>
                        <input
                          type="checkbox"
                          checked={campaignForm.filter_config.filials.includes(filial)}
                          onChange={(e) => {
                            const current = campaignForm.filter_config.filials;
                            const updated = e.target.checked 
                              ? [...current, filial]
                              : current.filter(f => f !== filial);
                            handleFilterChange('filials', updated);
                          }}
                          style={{marginRight: '6px'}}
                        />
                        {filial}
                      </label>
                    ))}
                  </div>
                  <small style={{color: '#6c757d'}}>
                    Включить только клиентов из выбранных филиалов. Выбрано: {campaignForm.filter_config.filials.length}
                  </small>
                </div>

                {/* Local Stream Control */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🎯 Исключение по локальным потокам</h4>
                  
                  <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px'}}>
                    <div>
                      <label style={{display: 'block', marginBottom: '8px', fontWeight: '500', fontSize: '14px'}}>
                        🚫 Control группы (исключить)
                      </label>
                      <div style={{border: '1px solid #ddd', borderRadius: '4px', padding: '8px', backgroundColor: 'white', maxHeight: '120px', overflowY: 'auto'}}>
                        {localStreams.map(stream => (
                          <label key={stream} style={{
                            display: 'flex',
                            alignItems: 'center',
                            padding: '3px',
                            cursor: 'pointer',
                            fontSize: '13px'
                          }}>
                            <input
                              type="checkbox"
                              checked={campaignForm.filter_config.local_control_streams.includes(stream)}
                              onChange={(e) => {
                                const current = campaignForm.filter_config.local_control_streams;
                                const updated = e.target.checked 
                                  ? [...current, stream]
                                  : current.filter(s => s !== stream);
                                handleFilterChange('local_control_streams', updated);
                              }}
                              style={{marginRight: '6px'}}
                            />
                            {stream}
                          </label>
                        ))}
                      </div>
                    </div>
                    
                    <div>
                      <label style={{display: 'block', marginBottom: '8px', fontWeight: '500', fontSize: '14px'}}>
                        🎯 Target группы (исключить)
                      </label>
                      <div style={{border: '1px solid #ddd', borderRadius: '4px', padding: '8px', backgroundColor: 'white', maxHeight: '120px', overflowY: 'auto'}}>
                        {localStreams.map(stream => (
                          <label key={stream} style={{
                            display: 'flex',
                            alignItems: 'center',
                            padding: '3px',
                            cursor: 'pointer',
                            fontSize: '13px'
                          }}>
                            <input
                              type="checkbox"
                              checked={campaignForm.filter_config.local_target_streams.includes(stream)}
                              onChange={(e) => {
                                const current = campaignForm.filter_config.local_target_streams;
                                const updated = e.target.checked 
                                  ? [...current, stream]
                                  : current.filter(s => s !== stream);
                                handleFilterChange('local_target_streams', updated);
                              }}
                              style={{marginRight: '6px'}}
                            />
                            {stream}
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>
                  
                  <small style={{color: '#6c757d'}}>
                    Исключить пользователей из активных кампаний в выбранных потоках. 
                    Control: {campaignForm.filter_config.local_control_streams.length}, 
                    Target: {campaignForm.filter_config.local_target_streams.length}
                  </small>
                </div>

                {/* RB3 Stream Control */}
                {campaignForm.campaign_type === 'RB3' && (
                  <div style={{marginBottom: '20px'}}>
                    <h4 style={{marginBottom: '10px', color: '#495057'}}>🏆 RB3 Исключение по потокам</h4>
                    
                    <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px'}}>
                      <div>
                        <label style={{display: 'block', marginBottom: '8px', fontWeight: '500', fontSize: '14px'}}>
                          🚫 RB3 Control (исключить)
                        </label>
                        <div style={{border: '1px solid #ddd', borderRadius: '4px', padding: '8px', backgroundColor: 'white', maxHeight: '120px', overflowY: 'auto'}}>
                          {rb3Streams.map(stream => (
                            <label key={stream} style={{
                              display: 'flex',
                              alignItems: 'center',
                              padding: '3px',
                              cursor: 'pointer',
                              fontSize: '13px'
                            }}>
                              <input
                                type="checkbox"
                                checked={campaignForm.filter_config.rb3_control_streams.includes(stream)}
                                onChange={(e) => {
                                  const current = campaignForm.filter_config.rb3_control_streams;
                                  const updated = e.target.checked 
                                    ? [...current, stream]
                                    : current.filter(s => s !== stream);
                                  handleFilterChange('rb3_control_streams', updated);
                                }}
                                style={{marginRight: '6px'}}
                              />
                              {stream}
                            </label>
                          ))}
                        </div>
                      </div>
                      
                      <div>
                        <label style={{display: 'block', marginBottom: '8px', fontWeight: '500', fontSize: '14px'}}>
                          🎯 RB3 Target (исключить)
                        </label>
                        <div style={{border: '1px solid #ddd', borderRadius: '4px', padding: '8px', backgroundColor: 'white', maxHeight: '120px', overflowY: 'auto'}}>
                          {rb3Streams.map(stream => (
                            <label key={stream} style={{
                              display: 'flex',
                              alignItems: 'center',
                              padding: '3px',
                              cursor: 'pointer',
                              fontSize: '13px'
                            }}>
                              <input
                                type="checkbox"
                                checked={campaignForm.filter_config.rb3_target_streams.includes(stream)}
                                onChange={(e) => {
                                  const current = campaignForm.filter_config.rb3_target_streams;
                                  const updated = e.target.checked 
                                    ? [...current, stream]
                                    : current.filter(s => s !== stream);
                                  handleFilterChange('rb3_target_streams', updated);
                                }}
                                style={{marginRight: '6px'}}
                              />
                              {stream}
                            </label>
                          ))}
                        </div>
                      </div>
                    </div>
                    
                    <small style={{color: '#6c757d'}}>
                      Исключить пользователей из активных RB3 кампаний в выбранных потоках.
                    </small>
                  </div>
                )}

                {/* Previous Campaign Cleanup */}
                <div style={{marginBottom: '20px'}}>
                  <h4 style={{marginBottom: '10px', color: '#495057'}}>🧹 Очистка от предыдущих кампаний</h4>
                  
                  <div style={{display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '15px', alignItems: 'end'}}>
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>
                        Коды кампаний для исключения (через запятую)
                      </label>
                      <input
                        type="text"
                        value={campaignForm.filter_config.previous_campaigns.join(', ')}
                        onChange={(e) => {
                          const campaigns = e.target.value.split(',').map(c => c.trim()).filter(c => c.length > 0);
                          handleFilterChange('previous_campaigns', campaigns);
                        }}
                        placeholder="C0000123456, C0000123457"
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      />
                    </div>
                    
                    <div>
                      <label style={{display: 'block', marginBottom: '5px', fontSize: '14px'}}>
                        Дата для дедупликации
                      </label>
                      <input
                        type="date"
                        value={campaignForm.filter_config.cleanup_date || ''}
                        onChange={(e) => handleFilterChange('cleanup_date', e.target.value || null)}
                        style={{width: '100%', padding: '8px', border: '1px solid #ddd', borderRadius: '4px'}}
                      />
                    </div>
                  </div>
                  
                  <small style={{color: '#6c757d'}}>
                    Исключить пользователей из указанных кампаний и дублирующиеся записи на выбранную дату.
                  </small>
                </div>

                {/* SMS Phone Requirement */}
                {campaignForm.metadata.channel === 'СМС' && (
                  <div style={{marginBottom: '20px'}}>
                    <h4 style={{marginBottom: '10px', color: '#495057'}}>📱 SMS настройки</h4>
                    <label style={{
                      display: 'flex',
                      alignItems: 'center',
                      padding: '12px 15px',
                      backgroundColor: 'white',
                      border: '1px solid #ddd',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      maxWidth: '350px'
                    }}>
                      <input
                        type="checkbox"
                        checked={campaignForm.filter_config.phone_required}
                        onChange={(e) => handleFilterChange('phone_required', e.target.checked)}
                        style={{marginRight: '10px'}}
                      />
                      <div>
                        <div style={{fontWeight: '500'}}>📲 Добавить номера телефонов</div>
                        <small style={{color: '#6c757d'}}>Загрузить проверенные номера для SMS</small>
                      </div>
                    </label>
                  </div>
                )}

                {/* Summary */}
                <div style={{
                  backgroundColor: '#e3f2fd',
                  border: '1px solid #2196f3',
                  borderRadius: '4px',
                  padding: '12px',
                  marginTop: '15px'
                }}>
                  <strong style={{color: '#1976d2'}}>📊 Сводка фильтров:</strong>
                  <ul style={{margin: '8px 0', paddingLeft: '20px', color: '#424242'}}>
                    <li>Стоп-листы: {campaignForm.filter_config.blacklist_tables.length} активных</li>
                    <li>Устройства: {campaignForm.filter_config.devices.length === 0 ? 'Все' : campaignForm.filter_config.devices.join(', ')}</li>
                    <li>Push-потоки: {campaignForm.filter_config.push_streams.length} исключений</li>
                    <li>MAU: {campaignForm.filter_config.mau_only ? 'Только активные' : 'Все пользователи'}</li>
                    <li>Возраст: {
                      campaignForm.filter_config.min_age || campaignForm.filter_config.max_age 
                        ? `${campaignForm.filter_config.min_age || 'от 18'} - ${campaignForm.filter_config.max_age || 'до 100'} лет`
                        : 'Без ограничений'
                    }</li>
                    <li>Филиалы: {campaignForm.filter_config.filials.length === 0 ? 'Все' : `${campaignForm.filter_config.filials.length} выбрано`}</li>
                    <li>Локальные потоки: Control {campaignForm.filter_config.local_control_streams.length}, Target {campaignForm.filter_config.local_target_streams.length}</li>
                    {campaignForm.campaign_type === 'RB3' && (
                      <li>RB3 потоки: Control {campaignForm.filter_config.rb3_control_streams.length}, Target {campaignForm.filter_config.rb3_target_streams.length}</li>
                    )}
                    <li>Предыдущие кампании: {campaignForm.filter_config.previous_campaigns.length} исключений</li>
                  </ul>
                </div>
              </div>

              {/* Create Button */}
              <div>
                <button 
                  onClick={createCampaign} 
                  disabled={loading}
                  style={{
                    backgroundColor: '#007bff',
                    color: 'white',
                    border: 'none',
                    padding: '12px 24px',
                    borderRadius: '4px',
                    cursor: loading ? 'not-allowed' : 'pointer',
                    fontSize: '16px'
                  }}
                >
                  {loading ? 'Создание...' : '🚀 Создать кампанию'}
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
};

export default CampaignManager;