# main.py - Discord Bot Ultimate Version (Updated: Kick Feature)
import os
import json
import asyncio
import threading
import discord
import aiohttp
import requests
from discord.ext import commands
from flask import Flask, request
from dotenv import load_dotenv
import time
from PIL import Image, ImageDraw
import io

# --- 1. CẤU HÌNH CƠ BẢN ---
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
CLIENT_ID = os.getenv('DISCORD_CLIENT_ID')
CLIENT_SECRET = os.getenv('DISCORD_CLIENT_SECRET')
DATABASE_URL = os.getenv('DATABASE_URL')
JSONBIN_API_KEY = os.getenv('JSONBIN_API_KEY')
JSONBIN_BIN_ID = os.getenv('JSONBIN_BIN_ID')
PORT = int(os.getenv('PORT', 5000))
RENDER_URL = os.getenv('RENDER_EXTERNAL_URL', f'http://127.0.0.1:{PORT}')
REDIRECT_URI = f'{RENDER_URL}/callback'

# Kiểm tra thư viện Database
try:
    import psycopg2
    HAS_PSYCOPG2 = True
    print("✅ Đã nạp module psycopg2 (PostgreSQL)")
except ImportError:
    HAS_PSYCOPG2 = False
    print("⚠️ Không có psycopg2, sẽ dùng JSONBin/File")

if not DISCORD_TOKEN: exit("LỖI: Thiếu DISCORD_TOKEN")

# --- 2. HỆ THỐNG LƯU TRỮ (STORAGE SYSTEM) ---

class JSONBinStorage:
    """Quản lý lưu trữ trên mây qua JSONBin.io"""
    def __init__(self):
        self.api_key = JSONBIN_API_KEY
        self.bin_id = JSONBIN_BIN_ID
        self.base_url = "https://api.jsonbin.io/v3"
        
    def _get_headers(self):
        return {"Content-Type": "application/json", "X-Master-Key": self.api_key, "X-Access-Key": self.api_key}
    
    def create_bin(self, data=None):
        if data is None: data = {}
        try:
            res = requests.post(f"{self.base_url}/b", json=data, headers=self._get_headers())
            if res.status_code == 200:
                self.bin_id = res.json()['metadata']['id']
                print(f"✅ Created new JSONBin: {self.bin_id}")
                return self.bin_id
        except Exception as e: print(f"❌ JSONBin Create Error: {e}")
        return None
    
    def read_data(self):
        if not self.bin_id: return {}
        try:
            res = requests.get(f"{self.base_url}/b/{self.bin_id}/latest", headers=self._get_headers())
            return res.json().get('record', {}) if res.status_code == 200 else {}
        except: return {}
    
    def write_data(self, data):
        if not self.bin_id: self.create_bin(data)
        try:
            requests.put(f"{self.base_url}/b/{self.bin_id}", json=data, headers=self._get_headers())
            return True
        except: return False

    def get_user_token(self, user_id):
        data = self.read_data()
        u = data.get(str(user_id))
        return u.get('access_token') if isinstance(u, dict) else u

    def save_user_token(self, user_id, access_token, username=None, avatar_hash=None):
        data = self.read_data()
        data[str(user_id)] = {'access_token': access_token, 'username': username, 'avatar_hash': avatar_hash, 'updated_at': str(time.time())}
        return self.write_data(data)
        
    def delete_user(self, user_id):
        data = self.read_data()
        if str(user_id) in data:
            del data[str(user_id)]
            return self.write_data(data)
        return True

jsonbin_storage = JSONBinStorage()

# --- DATABASE HELPERS ---
def get_db_connection():
    if DATABASE_URL and HAS_PSYCOPG2:
        try: return psycopg2.connect(DATABASE_URL, sslmode='require')
        except: return None
    return None

def init_database():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS user_tokens (user_id VARCHAR(50) PRIMARY KEY, access_token TEXT NOT NULL, username VARCHAR(100), avatar_hash TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            conn.commit()
            conn.close()
            print("✅ Database initialized")
        except Exception as e: print(f"❌ DB Init Error: {e}")

def get_user_access_token(user_id):
    # 1. Thử lấy từ Database
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT access_token FROM user_tokens WHERE user_id = %s", (str(user_id),))
            res = cur.fetchone()
            conn.close()
            if res: return res[0]
        except: conn.close()
    
    # 2. Thử lấy từ JSONBin
    if JSONBIN_API_KEY:
        return jsonbin_storage.get_user_token(user_id)
        
    # 3. Thử lấy từ file local
    try:
        with open('tokens.json', 'r') as f:
            return json.load(f).get(str(user_id), {}).get('access_token')
    except: return None

def save_user_token(user_id, access_token, username=None, avatar_hash=None):
    # Lưu vào DB
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('''INSERT INTO user_tokens (user_id, access_token, username, avatar_hash) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET access_token = EXCLUDED.access_token, username = EXCLUDED.username, avatar_hash = EXCLUDED.avatar_hash, updated_at = CURRENT_TIMESTAMP''', (str(user_id), access_token, username, avatar_hash))
            conn.commit()
            conn.close()
        except: conn.close()
        
    # Lưu vào JSONBin
    if JSONBIN_API_KEY:
        jsonbin_storage.save_user_token(user_id, access_token, username, avatar_hash)

    # Lưu file local backup
    try:
        try: 
            with open('tokens.json', 'r') as f: tokens = json.load(f)
        except: tokens = {}
        tokens[str(user_id)] = {'access_token': access_token, 'username': username, 'avatar_hash': avatar_hash}
        with open('tokens.json', 'w') as f: json.dump(tokens, f)
    except: pass
    return True

def delete_user_data(user_id):
    # Xóa DB
    conn = get_db_connection()
    if conn:
        try: 
            cur = conn.cursor(); cur.execute("DELETE FROM user_tokens WHERE user_id = %s", (str(user_id),)); conn.commit(); conn.close()
        except: conn.close()
    # Xóa JSONBin
    if JSONBIN_API_KEY: jsonbin_storage.delete_user(user_id)
    # Xóa Local
    try:
        with open('tokens.json', 'r') as f: tokens = json.load(f)
        if str(user_id) in tokens: del tokens[str(user_id)]
        with open('tokens.json', 'w') as f: json.dump(tokens, f)
    except: pass

# --- 3. KHỞI TẠO BOT ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

# === ID CHỦ BOT ĐƯỢC CẬP NHẬT TẠI ĐÂY ===
bot = commands.Bot(command_prefix='!', intents=intents, owner_id=970585437599072266, help_command=None)
app = Flask(__name__)

async def add_member_to_guild(guild_id, user_id, access_token):
    """API call để thêm user vào guild"""
    url = f"https://discord.com/api/v10/guilds/{guild_id}/members/{user_id}"
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}", "Content-Type": "application/json"}
    data = {"access_token": access_token}
    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=data) as response:
            if response.status == 201: return True, "Đã thêm mới"
            elif response.status == 204: return True, "Đã có trong server"
            else: return False, await response.text()

# --- 4. CÁC UI CLASS (GIAO DIỆN NÚT BẤM) ---
class RosterPages(discord.ui.View):
    def __init__(self, agents, ctx):
        super().__init__(timeout=180)
        self.agents = agents
        self.ctx = ctx
        self.current_page = 0
        self.items_per_page = 6
        self.total_pages = (len(self.agents) + self.items_per_page - 1) // self.items_per_page

    async def create_page_embed(self, page_num):
        start = page_num * self.items_per_page
        page_agents = self.agents[start:start + self.items_per_page]
        desc = "\n".join([f"👤 **{a['username']}** `(ID: {a['id']})`" for a in page_agents])
        embed = discord.Embed(title=f"Danh Sách ({len(self.agents)} User)", description=desc, color=0x2f3136)
        embed.set_footer(text=f"Trang {self.current_page + 1}/{self.total_pages}")
        return embed

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction, button):
        if self.current_page > 0:
            self.current_page -= 1
            embed = await self.create_page_embed(self.current_page)
            await interaction.response.edit_message(embed=embed)

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction, button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            embed = await self.create_page_embed(self.current_page)
            await interaction.response.edit_message(embed=embed)

# --- 5. LỆNH BOT (COMMANDS) ---

@bot.event
async def on_ready():
    print(f'✅ Bot đã online: {bot.user.name}')
    print(f'👑 Owner ID: {bot.owner_id}')
    try: await bot.tree.sync()
    except: pass

@bot.command()
async def auth(ctx):
    """Lấy link ủy quyền"""
    url = f'https://discord.com/api/oauth2/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=identify%20guilds.join'
    embed = discord.Embed(title="🔐 Xác Thực Tài Khoản", description=f"**[Nhấp vào đây để ủy quyền]({url})**\n\nLink này an toàn, dùng để bot có thể kéo bạn vào server.", color=0x00ff00)
    await ctx.send(embed=embed)

@bot.command()
async def check(ctx):
    """Kiểm tra xem đã có token chưa"""
    if get_user_access_token(ctx.author.id): await ctx.send("✅ Bot đã có dữ liệu của bạn.")
    else: await ctx.send("❌ Bạn chưa ủy quyền. Hãy dùng `!auth`.")

# --- TÍNH NĂNG 1: TỰ THÊM BẢN THÂN VÀO ALL SERVER (CÓ DELAY) ---
@bot.command()
async def add_me(ctx):
    token = get_user_access_token(ctx.author.id)
    if not token: return await ctx.send("❌ Chưa có token. Dùng `!auth` trước.")
    
    msg = await ctx.send(f"🔄 Đang thêm bạn vào {len(bot.guilds)} server... (Delay 2s/server)")
    count = 0
    for guild in bot.guilds:
        await asyncio.sleep(2) # DELAY 2 GIÂY
        try:
            if not guild.get_member(ctx.author.id):
                s, _ = await add_member_to_guild(guild.id, ctx.author.id, token)
                if s: count += 1
        except: pass
    await msg.edit(content=f"✅ Hoàn tất. Đã thêm vào {count} server mới.")

# --- TÍNH NĂNG 2: CHỦ BOT KÉO NGƯỜI KHÁC VÀO ALL SERVER (CÓ DELAY) ---
@bot.command(name='force_add')
@commands.is_owner()
async def force_add(ctx, user: discord.User):
    """
    Kéo user khác vào toàn bộ server.
    Cách dùng: !force_add <ID_User> hoặc !force_add @User
    """
    token = get_user_access_token(user.id)
    if not token: return await ctx.send(f"❌ User **{user.name}** chưa ủy quyền cho bot.")
    
    msg = await ctx.send(f"🚀 Đang kéo **{user.name}** vào {len(bot.guilds)} server...\n⏳ Tốc độ: 2 giây/server (để tránh bị chặn).")
    success = 0
    fail = 0
    
    for guild in bot.guilds:
        await asyncio.sleep(2) # DELAY 2 GIÂY QUAN TRỌNG
        try:
            # Kiểm tra xem đã có trong server chưa
            if guild.get_member(user.id):
                success += 1
            else:
                s, m = await add_member_to_guild(guild.id, user.id, token)
                if s: success += 1
                else: fail += 1
        except:
            fail += 1
            
    await msg.edit(content=f"📊 **Kết quả cho {user.name}:**\n✅ Thành công/Đã có mặt: **{success}** server\n❌ Thất bại: **{fail}** server")

# --- TÍNH NĂNG 3: TẠO KÊNH TOÀN SERVER (CÓ DELAY & CHECK TRÙNG) ---
@bot.command(name='vhoang')
@commands.is_owner()
async def vhoang(ctx, *, channel_name: str = None):
    """
    Tạo kênh trên toàn server.
    Cách dùng: !vhoang <tên_kênh>
    """
    if not channel_name: return await ctx.send("❌ Nhập tên kênh đi: `!vhoang spam-chat`")
    
    target_name = channel_name.lower().strip().replace(" ", "-")
    msg = await ctx.send(f"🔄 Đang tạo kênh **#{target_name}** trên toàn hệ thống... (Delay 2s)")
    
    created = 0
    skipped = 0
    
    for guild in bot.guilds:
        await asyncio.sleep(2) # DELAY 2 GIÂY
        try:
            # Kiểm tra xem kênh đã có chưa
            existing = discord.utils.get(guild.text_channels, name=target_name)
            if existing:
                skipped += 1
            else:
                await guild.create_text_channel(target_name, reason=f"Admin {ctx.author} ran !vhoang")
                created += 1
        except: pass
        
    await msg.edit(content=f"✅ **Xong!**\n🆕 Tạo mới: {created} server\n⏭️ Đã có sẵn: {skipped} server")

# --- TÍNH NĂNG 4: AUTO TẠO ROLE & CHẶN KÊNH TOÀN SERVER (CÓ DELAY) ---
@bot.command(name='block_spam')
@commands.is_owner()
async def block_spam(ctx, target_user: discord.User, role_name: str, channel_name: str):
    """
    Tạo role, gán cho user, và chặn user xem kênh đó.
    Cách dùng: !block_spam <User> <Role> <Channel>
    """
    msg = await ctx.send(f"🛡️ Đang xử lý chặn **{target_user.name}** khỏi kênh **{channel_name}**... (Delay 2s)")
    
    formatted_channel = channel_name.lower().strip().replace(" ", "-")
    success_count = 0
    
    for guild in bot.guilds:
        await asyncio.sleep(2) # DELAY 2 GIÂY
        try:
            # 1. Check member
            member = guild.get_member(target_user.id)
            if not member: continue
            
            # 2. Xử lý Role (Tìm hoặc Tạo)
            role = discord.utils.get(guild.roles, name=role_name)
            if not role:
                role = await guild.create_role(name=role_name, reason="Auto Block Spam")
            
            # 3. Gán Role
            if role not in member.roles:
                await member.add_roles(role)
                
            # 4. Chặn Kênh
            channel = discord.utils.get(guild.text_channels, name=formatted_channel)
            if channel:
                # Set permission: Role này không được xem kênh
                await channel.set_permissions(role, view_channel=False, send_messages=False)
                success_count += 1
                
        except: pass

    await msg.edit(content=f"✅ Đã thiết lập chặn trên **{success_count}** server có mặt user đó.")

# --- TÍNH NĂNG 5 (MỚI): KICK USER KHỎI TOÀN BỘ SERVER (CÓ DELAY) ---
@bot.command(name='kick_all')
@commands.is_owner()
async def kick_all(ctx, target_user: discord.User):
    """
    Đuổi (Kick) một thành viên khỏi TOÀN BỘ server mà bot tham gia.
    Cách dùng: !kick_all <User_ID> hoặc !kick_all @User
    """
    msg = await ctx.send(f"🦶 Đang thực hiện **Kick** user **{target_user.name}** khỏi toàn bộ server... (Delay 2s)")
    
    success = 0
    fail = 0
    
    for guild in bot.guilds:
        await asyncio.sleep(2) # DELAY 2 GIÂY AN TOÀN
        try:
            member = guild.get_member(target_user.id)
            if member:
                await member.kick(reason=f"Global Kick command by {ctx.author}")
                success += 1
            else:
                pass # User không có trong server này
        except discord.Forbidden:
            fail += 1 # Bot không có quyền kick (hoặc role thấp hơn)
        except:
            fail += 1
            
    await msg.edit(content=f"✅ **Hoàn tất Kick!**\n👟 Đã kick: **{success}** server\n❌ Thất bại (Thiếu quyền/Lỗi): **{fail}** server")

# --- CÁC LỆNH QUẢN LÝ KHÁC ---
@bot.command()
@commands.is_owner()
async def roster(ctx):
    """Xem danh sách user đã lưu"""
    data = jsonbin_storage.read_data()
    agents = [{'id': k, 'username': v.get('username', 'N/A')} for k,v in data.items()]
    if not agents: return await ctx.send("❌ Chưa có dữ liệu nào.")
    view = RosterPages(agents, ctx)
    embed = await view.create_page_embed(0)
    await ctx.send(embed=embed, view=view)

@bot.command()
@commands.is_owner()
async def remove(ctx, user: discord.User):
    """Xóa user khỏi database (Không phải kick)"""
    delete_user_data(user.id)
    await ctx.send(f"🗑️ Đã xóa dữ liệu lưu trữ của {user.name}")

@bot.command()
async def help(ctx):
    desc = """
    **Lệnh Cơ Bản:**
    `!auth` : Lấy link ủy quyền.
    `!check` : Kiểm tra trạng thái.
    `!add_me` : Tự thêm mình vào all server.
    
    **Lệnh Admin (Chỉ Chủ Bot):**
    `!force_add <User>` : Kéo user vào all server.
    `!vhoang <tên_kênh>` : Tạo kênh toàn server.
    `!block_spam <User> <Role> <Kênh>` : Chặn kênh toàn server.
    `!kick_all <User>` : Kick user khỏi toàn bộ server.
    `!roster` : Xem danh sách user.
    `!remove <User>` : Xóa user khỏi data.
    """
    await ctx.send(embed=discord.Embed(title="Danh Sách Lệnh", description=desc, color=0x0099ff))

# --- 6. FLASK WEB SERVER (GIAO DIỆN ỦY QUYỀN) ---
@app.route('/')
def index():
    auth_url = f'https://discord.com/api/oauth2/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=identify%20guilds.join'
    return f'''
    <html>
    <body style="background-color: #2c2f33; color: white; font-family: sans-serif; text-align: center; padding-top: 50px;">
        <h1>DISCORD BOT CONTROL</h1>
        <p>Hệ thống quản lý tự động.</p>
        <a href="{auth_url}" style="background-color: #7289da; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-size: 20px;">ĐĂNG NHẬP ỦY QUYỀN (LOGIN)</a>
    </body>
    </html>
    '''

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code: return "Lỗi: Không có code."
    
    # Đổi code lấy token
    data = {
        'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET,
        'grant_type': 'authorization_code', 'code': code, 'redirect_uri': REDIRECT_URI
    }
    r = requests.post('https://discord.com/api/v10/oauth2/token', data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    if r.status_code != 200: return f"Lỗi lấy token: {r.text}"
    token_data = r.json()
    
    # Lấy info user
    u = requests.get('https://discord.com/api/v10/users/@me', headers={'Authorization': f"Bearer {token_data['access_token']}"}).json()
    
    # Lưu lại
    save_user_token(u['id'], token_data['access_token'], u['username'], u.get('avatar'))
    
    return f'''
    <html>
    <body style="background-color: #2c2f33; color: white; font-family: sans-serif; text-align: center; padding-top: 50px;">
        <h1 style="color: #43b581;">THÀNH CÔNG!</h1>
        <p>Đã lưu dữ liệu cho user: <strong>{u['username']}</strong> (ID: {u['id']})</p>
        <p>Bây giờ bạn có thể đóng tab này.</p>
    </body>
    </html>
    '''

@app.route('/health')
def health(): return "OK", 200

def run_flask():
    app.run(host='0.0.0.0', port=PORT, debug=False)

# --- 7. CHẠY CHƯƠNG TRÌNH ---
if __name__ == '__main__':
    print("🚀 Khởi động hệ thống...")
    init_database()
    
    # Chạy Web Server ở luồng riêng
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    
    # Chạy Bot
    try:
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        print(f"❌ Lỗi chạy bot: {e}")
