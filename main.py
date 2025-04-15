from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import happybase
import uuid # 난수 생성 도와줌

class Chatroom(BaseModel): # BaseModel 상속한 Chatroom 모델 선언
    room_name: str

class Message(BaseModel): # BaseModel 상속한 Chatroom 모델 선언
    room_id: str
    content: str

# happybase
connection = happybase.Connection('localhost', port=9090)
    # happybase 프로그램 통해 thrift 서버와 연결
connection.open()

# fastapi
app = FastAPI() # 인스턴스

@app.get('/') # urls.py와 동일 역할하는 데코레이터: get요청으로 들어오는 슬래시 요청이란 의미
def index(): # views.py 역할
    return {'hello': 'world'}

## 채팅 서비스 만들기
# 채팅방 생성
@app.post('/chatrooms') # post 방식으로 /chatrooms 요청 들어오면, 채팅방 만들어줄 것
def create_chatroom(chatroom: Chatroom): # 입력한 room_name에 맞는 Chatroom 불러오기
    # hbase table와 연결
    table = connection.table('chatrooms')
    chatroom_id = str(uuid.uuid4()) # 난수로 id 만들기
    table.put(chatroom_id, {'info:room_name': chatroom.room_name})
    return {
        'chatroom_id': chatroom_id,
        'room_name': chatroom.room_name}

# 현재 생성된 채팅방 목록 조회
@app.get('/chatrooms')
def get_chatrooms():
    table = connection.table('chatrooms') # 테이블 연결
    rows = table.scan() # hbase의 scan과 동일 기능

    result = []

    for k, v in rows:
        result.append(
            {'chatroom_id': k,
            'room_name': v[b'info:room_name'],} # b: bite 형식으로 인코딩된 문자열이란 것
        )
    return result
    
# 각 채팅방에 채팅 만들기
@app.post('/messages')
def create_messages(message: Message): # 변수 message에 클래스 Message 넣기
    table = connection.table('messages') # 메시지 테이블과 연결
    room_id = message.room_id
    timestamp = int(datetime.now().timestamp() * 1000) # 밀리세컨드마다 찍히는 타임스탬프 값을 m_id에 붙일 것
    message_id = f'{room_id}-{timestamp}'

    table.put(message_id, {'info:content': message.content, 'info:room_id': room_id})
        # 인자: id값, 어떤 컬럼에 어떤 값 넣을지
    
    return { # 사용자에게 보여줄 값
        'message_id': message_id ,
        'room_id': room_id,
        'content': message.content,
    }

# 특정 방에 포함된 모든 메시지 조회
@app.get('/chatrooms/{room_id}/messages')
def get_messages(room_id: str):
    table = connection.table('messages')
    prefix = room_id.encode('utf-8') # 인코딩 방식 변경

    # 앞글자 room_id인 메시지만 추출
    rows = table.scan(row_prefix=prefix, reverse=True) # 최근 메시지를 위로 올리기
    
    result = []

    for k, v in rows:
        result.append({
            'message_id': k,
            'room_id': v[b'info:room_id'],
            'content': v[b'info:content'],
        })
    return result