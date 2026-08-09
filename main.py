from flask import Flask, request, jsonify
import websocket, json, os, time, sqlite3, threading, urllib.request, urllib.parse

app = Flask(__name__)
DERIV_APP_ID=os.environ.get('DERIV_APP_ID','YOUR_APP_ID_HERE')
DERIV_WS_URL=f'wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}'
TELEGRAM_BOT_TOKEN=os.environ.get('TELEGRAM_BOT_TOKEN','')
ADMIN_KEY=os.environ.get('ADMIN_KEY','')
DB_PATH=os.environ.get('DB_PATH','structure_memory.db')
TIMEFRAME_MAP={'M1':60,'M5':300,'M15':900,'M30':1800,'H1':3600,'H4':14400,'D1':86400}
SYMBOL_MAP={'VOLATILITY10':'R_10','VOLATILITY25':'R_25','VOLATILITY50':'R_50','VOLATILITY75':'R_75','VOLATILITY100':'R_100','BOOM500':'BOOM500','BOOM1000':'BOOM1000','CRASH500':'CRASH500','CRASH1000':'CRASH1000'}
lock=threading.Lock()

def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute('''CREATE TABLE IF NOT EXISTS structures(id INTEGER PRIMARY KEY AUTOINCREMENT,structure_key TEXT UNIQUE,pair TEXT,timeframe TEXT,direction TEXT,state TEXT,a_json TEXT,b_json TEXT,c_json TEXT,d_json TEXT,e_json TEXT,f_json TEXT,fib50 REAL,valid INTEGER DEFAULT 0,telegram_sent INTEGER DEFAULT 0,created_epoch INTEGER,updated_epoch INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS telegram_users(chat_id TEXT PRIMARY KEY,username TEXT,active INTEGER DEFAULT 1,created_epoch INTEGER)''')
    c.commit(); return c

def candles(symbol,granularity,count=500):
    result=[]; err=None; done=threading.Event()
    def msg(ws,m):
        nonlocal result,err
        try:
            d=json.loads(m)
            if 'candles' in d: result=d['candles']; done.set()
            elif 'error' in d: err=d['error'].get('message',str(d['error'])); done.set()
        except Exception as e: err=str(e); done.set()
    def error(ws,e):
        nonlocal err; err=str(e); done.set()
    def opened(ws):
        ws.send(json.dumps({'ticks_history':symbol,'adjust_start_time':1,'count':max(100,min(int(count),1000)),'granularity':granularity,'style':'candles','end':'latest'}))
    ws=websocket.WebSocketApp(DERIV_WS_URL,on_open=opened,on_message=msg,on_error=error)
    threading.Thread(target=ws.run_forever,daemon=True).start(); done.wait(25)
    try: ws.close()
    except: pass
    if err: print('[Deriv]',err)
    return sorted(result,key=lambda x:int(x.get('epoch',0)))

def norm(rows):
    return [{'epoch':int(x.get('epoch',0)),'open':float(x['open']),'high':float(x['high']),'low':float(x['low']),'close':float(x['close']),'volume':float(x.get('volume',0))} for x in rows]

def atrs(cs,p=14):
    a=[0.0]*len(cs); tr=[]
    for i,x in enumerate(cs):
        if i==0: tr.append(x['high']-x['low'])
        else:
            q=cs[i-1]; tr.append(max(x['high']-x['low'],abs(x['high']-q['close']),abs(x['low']-q['close'])))
    for i in range(p,len(cs)): a[i]=sum(tr[i-p+1:i+1])/p
    return a

def swings(cs,s=3):
    out=[]; s=max(1,int(s))
    for i in range(s,len(cs)-s):
        x=cs[i]
        if all(x['low']<y['low'] for y in cs[i-s:i]) and all(x['low']<=y['low'] for y in cs[i+1:i+s+1]): out.append(('L',i,x))
        if all(x['high']>y['high'] for y in cs[i-s:i]) and all(x['high']>=y['high'] for y in cs[i+1:i+s+1]): out.append(('H',i,x))
    return sorted(out,key=lambda z:z[1])

def pt(label,role,c,price):
    return {'label':label,'role':role,'price':float(price),'epoch':int(c['epoch']),'open':c['open'],'high':c['high'],'low':c['low'],'close':c['close']}

def key(s): return f"{s['pair']}|{s['timeframe']}|{s['direction']}|{s['a']['epoch']}|{s['b']['epoch']}|{s['c']['epoch']}"

def fib50(b,e,d): return b+(e-b)*.5 if d=='BULLISH' else b-(b-e)*.5

def save(s):
    now=int(time.time()); k=key(s)
    def j(x): return json.dumps(x,separators=(',',':')) if x else None
    with lock:
        c=db(); c.execute('''INSERT INTO structures(structure_key,pair,timeframe,direction,state,a_json,b_json,c_json,d_json,e_json,f_json,fib50,valid,created_epoch,updated_epoch) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(structure_key) DO UPDATE SET state=excluded.state,d_json=excluded.d_json,e_json=excluded.e_json,f_json=excluded.f_json,fib50=excluded.fib50,valid=excluded.valid,updated_epoch=excluded.updated_epoch''',(k,s['pair'],s['timeframe'],s['direction'],s['state'],j(s['a']),j(s['b']),j(s['c']),j(s['d']),j(s['e']),j(s['f']),s.get('fib50'),int(s.get('valid',False)),s['a']['epoch'],now)); c.commit(); c.close()
    return k

def row_to_s(r):
    def p(n): return json.loads(r[n]) if r[n] else None
    return {'id':r['id'],'structure_key':r['structure_key'],'pair':r['pair'],'timeframe':r['timeframe'],'direction':r['direction'],'state':r['state'],'a':p('a_json'),'b':p('b_json'),'c':p('c_json'),'d':p('d_json'),'e':p('e_json'),'f':p('f_json'),'fib50':r['fib50'],'valid':bool(r['valid']),'telegram_sent':bool(r['telegram_sent'])}

def active(pair,tf):
    with lock:
        c=db(); rows=c.execute("SELECT * FROM structures WHERE pair=? AND timeframe=? AND state!='COMPLETE' ORDER BY created_epoch",(pair,tf)).fetchall(); c.close()
    return [row_to_s(r) for r in rows]

def new_abc(cs,pair,tf,direction,strength,min_atr):
    sw=swings(cs,strength); lows=[x for x in sw if x[0]=='L']; highs=[x for x in sw if x[0]=='H']; av=atrs(cs); out=[]
    if direction=='BULLISH':
        for a in lows:
            for b in lows:
                if b[1]<=a[1] or b[2]['low']<=a[2]['low']: continue
                if min_atr and av[b[1]] and b[2]['low']-a[2]['low']<av[b[1]]*min_atr: continue
                c=next((x for x in highs if x[1]>b[1]),None)
                if c:
                    out.append({'pair':pair,'timeframe':tf,'direction':direction,'a':pt('A','SWEEP',a[2],a[2]['low']),'b':pt('B','SWING',b[2],b[2]['low']),'c':pt('C','STRUCTURE',c[2],c[2]['high']),'d':None,'e':None,'f':None,'fib50':None,'state':'WAITING_FOR_BOS','valid':False}); break
    else:
        for a in highs:
            for b in highs:
                if b[1]<=a[1] or b[2]['high']>=a[2]['high']: continue
                if min_atr and av[b[1]] and a[2]['high']-b[2]['high']<av[b[1]]*min_atr: continue
                c=next((x for x in lows if x[1]>b[1]),None)
                if c:
                    out.append({'pair':pair,'timeframe':tf,'direction':direction,'a':pt('A','SWEEP',a[2],a[2]['high']),'b':pt('B','SWING',b[2],b[2]['high']),'c':pt('C','STRUCTURE',c[2],c[2]['low']),'d':None,'e':None,'f':None,'fib50':None,'state':'WAITING_FOR_BOS','valid':False}); break
    return out

def advance(s,cs,bos='body',exp_atr=.5,disp_atr=1.0):
    ix={x['epoch']:i for i,x in enumerate(cs)}; ci=ix.get(s['c']['epoch']); av=atrs(cs)
    if ci is None: return s
    d=s['direction']; level=s['c']['price']
    if s['d'] is None:
        for i in range(ci+1,len(cs)):
            x=cs[i]; broken=(x['close']>level if d=='BULLISH' else x['close']<level) if bos=='body' else (x['high']>level if d=='BULLISH' else x['low']<level)
            if broken:
                s['d']=pt('D','BOS',x,x['high'] if d=='BULLISH' else x['low']); s['state']='WAITING_FOR_EXPANSION'; break
    if not s['d']: return s
    di=ix.get(s['d']['epoch']);
    if di is None: return s
    if s['e'] is None:
        best=None
        for i in range(di+1,len(cs)):
            x=cs[i]; move=x['high']-level if d=='BULLISH' else level-x['low']
            if move<=0 or (av[di] and move<av[di]*exp_atr): continue
            better=best is None or (x['high']>best[1]['high'] if d=='BULLISH' else x['low']<best[1]['low'])
            if better: best=(i,x)
        if best:
            x=best[1]; s['e']=pt('E','EXPANSION',x,x['high'] if d=='BULLISH' else x['low']); s['state']='WAITING_FOR_DISPLACEMENT'
    if not s['e']: return s
    ei=ix.get(s['e']['epoch']);
    if ei is None: return s
    mid=fib50(s['b']['price'],s['e']['price'],d)
    if s['f'] is None:
        for i in range(ei+1,len(cs)):
            x=cs[i]; f=x['low'] if d=='BULLISH' else x['high']; reaches=f<=mid if d=='BULLISH' else f>=mid
            rng=x['high']-x['low']; body=abs(x['close']-x['open'])
            if not reaches or not av[i] or rng<av[i]*1.1 or body<av[i]*disp_atr: continue
            s['f']=pt('F','DISPLACEMENT',x,f); s['fib50']=mid; s['valid']=True; s['state']='COMPLETE'; break
    return s

def tg_send(chat,text):
    if not TELEGRAM_BOT_TOKEN: return False
    data=urllib.parse.urlencode({'chat_id':str(chat),'text':text,'parse_mode':'HTML'}).encode(); req=urllib.request.Request(f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',data=data)
    try: urllib.request.urlopen(req,timeout=10).read(); return True
    except Exception as e: print('[Telegram]',e); return False

def signal_text(s):
    icon='🟢' if s['direction']=='BULLISH' else '🔴'; lines=[f'{icon} <b>{s["direction"]} A→F STRUCTURE COMPLETED</b>','',f'<b>Symbol:</b> {s["pair"]}',f'<b>Timeframe:</b> {s["timeframe"]}','']
    for k in ('a','b','c','d','e','f'):
        p=s[k]; lines += [f'<b>{k.upper()} — {p["role"]}</b>',f'Price: <code>{p["price"]}</code>',f'Time: {time.strftime("%Y-%m-%d %H:%M:%S UTC",time.gmtime(p["epoch"]))}','']
    lines += ['━━━━━━━━━━━━━━━━━━',f'<b>50% Fibonacci:</b> <code>{s["fib50"]}</code>','', '<b>ENTRY:</b> Manual','<b>TP:</b> Manual','<b>SL:</b> Manual','', 'Structure information only. No trade decision is made by the engine.']
    return '\n'.join(lines)

def alert(s):
    k=key(s)
    with lock:
        c=db(); r=c.execute('SELECT telegram_sent FROM structures WHERE structure_key=?',(k,)).fetchone(); c.close()
    if r and r['telegram_sent']: return 0
    with lock:
        c=db(); users=c.execute('SELECT chat_id FROM telegram_users WHERE active=1').fetchall(); c.close()
    sent=sum(1 for u in users if tg_send(u['chat_id'],signal_text(s)))
    if sent:
        with lock:
            c=db(); c.execute('UPDATE structures SET telegram_sent=1,updated_epoch=? WHERE structure_key=?',(int(time.time()),k)); c.commit(); c.close()
    return sent

def run_scan(pair,tf,cs,strength,bos,min_atr,exp_atr,disp_atr):
    mem=active(pair,tf); keys={x['structure_key'] for x in mem}
    for d in ('BULLISH','BEARISH'):
        for s in new_abc(cs,pair,tf,d,strength,min_atr):
            k=key(s)
            if k not in keys: save(s); mem.append(s); keys.add(k)
    completed=[]
    for s in mem:
        old=s['state']; s=advance(s,cs,bos,exp_atr,disp_atr); save(s)
        if old!='COMPLETE' and s['state']=='COMPLETE':
            n=alert(s); s['telegram_sent_now']=bool(n); completed.append(s)
    return {'pair':pair,'timeframe':tf,'scan_mode':'DUAL_DIRECTION','memory_enabled':True,'state_machine':True,'active_structures':len(mem),'completed_now':len(completed),'signals':mem,'completed_signals':completed}

@app.route('/')
def home(): return 'Deriv Proxy v5 — Freedom Structure Scanner V2 is running ✅'
@app.route('/health')
def health(): return jsonify({'ok':True,'engine':'Freedom Structure Scanner V2','memory':True,'telegram_configured':bool(TELEGRAM_BOT_TOKEN),'timeframes':list(TIMEFRAME_MAP)})
@app.route('/ohlc')
def ohlc():
    pair=request.args.get('pair','').upper(); tf=request.args.get('timeframe','M15').upper(); count=int(request.args.get('count',500))
    if tf not in TIMEFRAME_MAP: return jsonify({'error':'Unsupported timeframe','supported':list(TIMEFRAME_MAP)}),400
    return jsonify(norm(candles(SYMBOL_MAP.get(pair,pair),TIMEFRAME_MAP[tf],count)))
@app.route('/scan')
def scan():
    pair=request.args.get('pair','').upper(); tf=request.args.get('timeframe','M15').upper(); count=int(request.args.get('count',500)); strength=int(request.args.get('strength',3)); bos=request.args.get('bos','body').lower(); bos=bos if bos in ('body','wick') else 'body'
    min_atr=float(request.args.get('min_atr_move',.25)); exp_atr=float(request.args.get('min_expansion_atr',.5)); disp_atr=float(request.args.get('displacement_atr',1.0))
    if tf not in TIMEFRAME_MAP: return jsonify({'error':'Unsupported timeframe','supported':list(TIMEFRAME_MAP)}),400
    cs=norm(candles(SYMBOL_MAP.get(pair,pair),TIMEFRAME_MAP[tf],count))
    if len(cs)<max(50,strength*4): return jsonify({'error':'Not enough candles returned','candles':len(cs)}),502
    return jsonify(run_scan(pair,tf,cs,strength,bos,min_atr,exp_atr,disp_atr))
@app.route('/structures')
def structures():
    pair=request.args.get('pair'); tf=request.args.get('timeframe'); sql='SELECT * FROM structures'; args=[]; w=[]
    if pair: w.append('pair=?'); args.append(pair.upper())
    if tf: w.append('timeframe=?'); args.append(tf.upper())
    if w: sql+=' WHERE '+' AND '.join(w)
    sql+=' ORDER BY updated_epoch DESC LIMIT 300'
    with lock:
        c=db(); rows=c.execute(sql,args).fetchall(); c.close()
    return jsonify([row_to_s(r) for r in rows])
@app.route('/telegram/register',methods=['POST'])
def register():
    d=request.get_json(force=True); chat=d.get('chat_id')
    if chat is None: return jsonify({'error':'chat_id required'}),400
    with lock:
        c=db(); c.execute('INSERT INTO telegram_users(chat_id,username,active,created_epoch) VALUES(?,?,1,?) ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,active=1',(str(chat),d.get('username',''),int(time.time()))); c.commit(); c.close()
    return jsonify({'ok':True,'chat_id':str(chat)})
@app.route('/telegram/users')
def users():
    if ADMIN_KEY and request.headers.get('X-Admin-Key')!=ADMIN_KEY: return jsonify({'error':'unauthorized'}),401
    with lock:
        c=db(); rows=c.execute('SELECT chat_id,username,active,created_epoch FROM telegram_users ORDER BY created_epoch DESC').fetchall(); c.close()
    return jsonify([dict(r) for r in rows])
@app.route('/telegram/broadcast',methods=['POST'])
def broadcast():
    if ADMIN_KEY and request.headers.get('X-Admin-Key')!=ADMIN_KEY: return jsonify({'error':'unauthorized'}),401
    text=request.get_json(force=True).get('text','')
    with lock:
        c=db(); users=c.execute('SELECT chat_id FROM telegram_users WHERE active=1').fetchall(); c.close()
    sent=sum(1 for u in users if tg_send(u['chat_id'],text)); return jsonify({'targeted':len(users),'sent':sent})
if __name__=='__main__': app.run(host='0.0.0.0',port=int(os.environ.get('PORT',8080)))
