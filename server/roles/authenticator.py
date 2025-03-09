from fastapi import APIRouter, Request, Body, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
import bcrypt
from datetime import datetime, timedelta, timezone

from utils.utils import hash_key
from utils.const import API_PORT

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


ALGORITHM = "HS256"
SECRET = "1652e68e6e5c4c9d21c6c38a87c143ea3f0b865fe318fae0374de808f5f0016f"
ACCESS_TOKEN_DURATION = 10


router = APIRouter()

oauth2 = OAuth2PasswordBearer(tokenUrl="login")


@router.post("/authenticate")
async def auth(request: Request, credentials: list = Body(...) ):

    # Validar formato de entrada
    if len(credentials) != 2 or not all(isinstance(item, str) for item in credentials):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Formato inválido. Debe ser un par (username, password)"
        )
    
    username = credentials[0]
  
    node = request.app.state.node

    try:

        # Determine responsible node using Chord
        key = hash_key(username)
        owner_node = node.find_succ(node.ref,key)        

        if owner_node.id != node.id:   
            logger.info(f"Redirecting auth request of {username} to responsible node: {owner_node.ip}")         
            url = f"http://{owner_node.ip}:{API_PORT}/authenticate"
            return RedirectResponse(url,status_code=307)
        
        if username in node.users_dict:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"User {username} already exist.")
        

        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(credentials[1].encode(), salt)
        
        node.users_dict[username] = hashed_password.decode('utf-8')
        logger.info(f"Usuario {username} registrado {hashed_password}") 
        
        access_token = {
            "sub": username,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_DURATION),            
        }     
     
        return {"access_token": jwt.encode(access_token, SECRET, algorithm=ALGORITHM), "token_type":"bearer"}  
    
    except HTTPException:
        raise  # Re-lanza las excepciones HTTP ya manejadas
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"message": f"Error interno del servidor: {str(e)}"}
        )


@router.post("/login")
async def login(request: Request, credentials: list = Body(...)):

    username = credentials[0]
    node = request.app.state.node

    try:

        # Determine responsible node using Chord
        key = hash_key(username)
        owner_node = node.find_succ(node.ref,key)        

        if owner_node.id != node.id:
            logger.info(f"Redirecting login request of {username} to responsible node: {owner_node.ip}")
            url = f"http://{owner_node.ip}:{API_PORT}/login"
            return RedirectResponse(url,status_code=307)
        
        
        if username not in node.users_dict:          
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User not found, please register."
            )    
        
        hashed_password = node.users_dict[username].encode('utf-8')
        if not bcrypt.checkpw(credentials[1].encode(), hashed_password):       
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Password do not match."
            )   
        
        access_token = {
            "sub": username,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_DURATION),            
        }     
     
        return {"access_token": jwt.encode(access_token, SECRET, algorithm=ALGORITHM), "token_type":"bearer"}  

    except HTTPException:
        raise  # Re-lanza las excepciones HTTP ya manejadas
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"message": f"Error interno del servidor: {str(e)}"}
        )




        





