"""Presentation for Telegram and plain-text consoles."""
from datetime import datetime, timedelta, timezone
from html import escape
from math import isfinite

RULE='━━━━━━━━━━━━━━'
KST=timezone(timedelta(hours=9))
HEADINGS=('📌 ','🟢 ','🔴 ','🎯 ','💰 ','📉 ','📈 ','⚠️ ','⏳ ','❌ ','📚 ','💬 ','📊 ','🛠 ','✅ ')

def timestamp(value):
    try:
        t=datetime.fromisoformat(value).astimezone(KST)
        return f'{t.month}월 {t.day}일 {t:%H:%M} KST'
    except (ValueError,TypeError):
        return '시각 확인 불가'

def payload(chat,body):
    # Budget plain UTF-16 length before escaping. Close bold tags on every line.
    lines=[]; used=0
    for line in body.split('\n'):
        length=len(line.encode('utf-16-le'))//2+1
        if used+length>3990:
            lines.append('… 길이 제한으로 나머지 생략')
            break
        escaped=escape(line,quote=False)
        lines.append(f'<b>{escaped}</b>' if line.startswith(HEADINGS) else escaped)
        used+=length
    return dict(chat_id=chat,text='\n'.join(lines),parse_mode='HTML')

def payloads(chat,body):
    """Split on line boundaries before HTML escaping, with a conservative UTF-16 budget."""
    limit=3900
    lines=[]; used=0
    for line in body.split('\n'):
        length=len(line.encode('utf-16-le'))//2+1
        if length>limit:
            if lines:
                yield payload(chat,'\n'.join(lines))
                lines=[]; used=0
            fragment=[]; units=0
            for char in line:
                width=2 if ord(char)>0xffff else 1
                if units+width>=limit:
                    yield payload(chat,''.join(fragment))
                    fragment=[]; units=0
                fragment.append(char)
                units+=width
            lines=[''.join(fragment)]; used=units+1
            continue
        if lines and used+length>limit:
            yield payload(chat,'\n'.join(lines))
            lines=[]; used=0
        lines.append(line)
        used+=length
    if lines:
        yield payload(chat,'\n'.join(lines))


def reason(label,m):
    f=m.get('indicators',{})
    def number(frame,key):
        value=f.get(frame,{}).get(key)
        return value if isinstance(value,(int,float)) and isfinite(value) else None
    def flow(value):
        return f'공격 매{"수" if value>=0 else "도"} {(1+abs(value))*50:.1f}%'
    if '선물 공격 수급' in label:
        values=[f'{tf[1:-1]}분 {flow(v)}' for tf in ('f5m','f15m') if (v:=number(tf,'flow')) is not None]
        if values: return '선물 수급 · '+' / '.join(values)
    if '현물 수급' in label and (v:=m.get('spot_flow')) is not None:
        return '현물 수급 · '+flow(v)
    if 'RSI' in label:
        r=number('f15m','rsi'); w=number('f15m','vwap')
        if r is not None and w is not None:
            compared='위' if m['price']>w else '아래' if m['price']<w else '수준'
            return f'15분 RSI {r:.1f} · 현재가가 VWAP {w:,.2f} {compared}'
    if 'EMA' in label:
        values=[f'{tf} {v:+.2%}' for tf in ('f1h','f4h') if (v:=number(tf,'ema')) is not None]
        if values: return 'EMA20/50 괴리 · '+', '.join(v.lstrip('f') for v in values)
    if '상대강도' in label and (v:=m.get('relative_strength')) is not None:
        return f'{label} {v:+.2%}'
    if 'MACD' in label and (v:=number('f5m','macd')) is not None:
        return f'5분 MACD 히스토그램/종가 {v:+.3%}'
    if '호가' in label and (v:=m.get('book_imbalance')) is not None:
        return f'호가 불균형 {v:+.1%}'
    if '지지·저항 접근' in label:
        return f"지지 {m['support']:,.2f} · 저항 {m['resistance']:,.2f}"
    return label

def notice(body):
    if body.startswith('등록 #'):
        title='⚠️ 등록 포지션 위험' if '강화' in body or '청산' in body else '🎯 등록 포지션 가격 도달'
        return title+'\n\n'+body
    if '정상 복구' in body: return '✅ 시스템 복구\n\n'+body
    if '분석 장애 지속' in body: return '🛠 시스템 장애\n\n'+body
    return body
