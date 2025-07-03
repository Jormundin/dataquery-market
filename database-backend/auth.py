"""
LDAP Authentication Module for DataQuery Pro
Adapted from Streamlit LDAP authentication
"""

import logging
import datetime
from typing import Optional, Dict, Any
from ldap3 import Server, Connection, ALL, NTLM, Tls
from fastapi import HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext
import os

# Configure logging
logging.basicConfig(
    filename='login_history.log', 
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# JWT Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-this-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 hours

# LDAP Configuration
LDAP_CONFIG = {
    'domain': 'UNIVERSAL',                      
    'host': 'ala200i26.halykbank.nb',            
    'port': 389,                            
    'base_dn': 'OU=OU_Users,DC=halykbank,DC=nb',      
    'use_ssl': False,                        
    'use_tls': False,                        
    'timeout': 10,                            
}

# Permitted users - hardcoded as requested
PERMITTED_USERS = {
    '00058215': {
        'name': 'Nadir',
        'role': 'admin',
        'permissions': ['read', 'write', 'admin']
    },
    # Add more users here - uncomment and modify these examples:
    # '00012345': {
    #     'name': 'John Doe',
    #     'role': 'user', 
    #     'permissions': ['read']
    # },
    # '00067890': {
    #     'name': 'Jane Smith',
    #     'role': 'analyst',
    #     'permissions': ['read', 'write']
    # },
    # '00054321': {
    #     'name': 'Bob Johnson',
    #     'role': 'admin',
    #     'permissions': ['read', 'write', 'admin']
    # }
}

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def check_ldap_password(username: str, password: str) -> bool:
    """
    Authenticate user against LDAP server
    Adapted from original Streamlit code
    """
    try:
        server = Server(
            LDAP_CONFIG['host'],
            port=LDAP_CONFIG['port'],
            use_ssl=LDAP_CONFIG['use_ssl'],
            get_info=ALL
        )

        if LDAP_CONFIG['use_tls']:
            tls_configuration = Tls(validate=False)  # ssl.CERT_NONE equivalent
            server.tls = tls_configuration

        user_dn = f"{LDAP_CONFIG['domain']}\\{username}"
        
        logging.info(f"Attempting to bind with user DN: {user_dn}")

        conn = Connection(
            server,
            user=user_dn,
            password=password,
            authentication=NTLM,
            auto_bind=True
        )

        if conn.bind():
            logging.info(f"Successfully authenticated user: {username}")
            conn.unbind()
            return True
        else:
            logging.error(f"Failed to authenticate user: {username}")
            return False
            
    except Exception as e:
        logging.error(f"LDAP authentication error: {e}")
        return False

def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify JWT token and return user data"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
        return payload
    except JWTError:
        return None

def authenticate_user(username: str, password: str) -> Dict[str, Any]:
    """
    Authenticate user and return user info
    """
    now = datetime.datetime.now()
    
    # Check if user is in permitted list
    if username not in PERMITTED_USERS:
        logging.info(f"Not approved user {username} at {now}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ к ресурсу отсутствует"
        )
    
    # Authenticate against LDAP
    if not check_ldap_password(username, password):
        logging.info(f"Incorrect login attempt for user {username} at {now}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="The username or password you have entered is incorrect."
        )
    
    # Get user info
    user_info = PERMITTED_USERS[username]
    logging.info(f"Successful login for user {username} ({user_info['name']}) at {now}")
    
    return {
        "username": username,
        "name": user_info['name'],
        "role": user_info['role'],
        "permissions": user_info['permissions']
    }

def get_current_user(token: str) -> Dict[str, Any]:
    """Get current user from token"""
    payload = verify_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    username = payload.get("sub")
    if username not in PERMITTED_USERS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized"
        )
    
    user_info = PERMITTED_USERS[username]
    return {
        "username": username,
        "name": user_info['name'],
        "role": user_info['role'],
        "permissions": user_info['permissions']
    } 