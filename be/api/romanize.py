# Simple Korean Romanization for KBO Names
# This provides a fallback for names that don't have a hardcoded English equivalent

CHO = {
    "ㄱ": "g", "ㄲ": "kk", "ㄴ": "n", "ㄷ": "d", "ㄸ": "tt", "ㄹ": "r",
    "ㅁ": "m", "ㅂ": "b", "ㅃ": "pp", "ㅅ": "s", "ㅆ": "ss", "ㅇ": "",
    "ㅈ": "j", "ㅉ": "jj", "ㅊ": "ch", "ㅋ": "k", "ㅌ": "t", "ㅍ": "p", "ㅎ": "h"
}

JUNG = {
    "ㅏ": "a", "ㅐ": "ae", "ㅑ": "ya", "ㅒ": "yae", "ㅓ": "eo", "ㅔ": "e",
    "ㅕ": "yeo", "ㅖ": "ye", "ㅗ": "o", "ㅘ": "wa", "ㅙ": "wae", "ㅚ": "oe",
    "ㅛ": "yo", "ㅜ": "u", "ㅝ": "wo", "ㅞ": "we", "ㅟ": "wi", "ㅠ": "yu",
    "ㅡ": "eu", "ㅢ": "ui", "ㅣ": "i"
}

JONG = {
    "": "", "ㄱ": "k", "ㄲ": "k", "ㄳ": "k", "ㄴ": "n", "ㄵ": "n", "ㄶ": "n",
    "ㄷ": "t", "ㄹ": "l", "ㄺ": "k", "ㄻ": "m", "ㄼ": "p", "ㄽ": "l", "ㄾ": "l",
    "ㄿ": "p", "ㅀ": "l", "ㅁ": "m", "ㅂ": "p", "ㅄ": "p", "ㅅ": "t", "ㅆ": "t",
    "ㅇ": "ng", "ㅈ": "t", "ㅊ": "t", "ㅋ": "k", "ㅌ": "t", "ㅍ": "p", "ㅎ": "t"
}

# Hardcoded prominent KBO players for common exceptions
EXCEPTIONS = {
    "김도영": "Kim Do-yeong",
    "구자욱": "Koo Ja-wook",
    "최정": "Choi Jeong",
    "이정후": "Lee Jung-hoo",
    "류현진": "Ryu Hyun-jin",
    "양의지": "Yang Eui-ji",
    "박병호": "Park Byung-ho",
    "에레디아": "Heredia",
    "소크라테스": "Socrates",
    "데이비슨": "Davidson",
    "오스틴": "Austin",
    "로하스": "Rojas",
    "구본혁": "Koo Bon-hyeok",
    "강백호": "Kang Baek-ho",
    "페라자": "Peraza",
    "도슨": "Dawson",
    "레이예스": "Reyes",
    # Teams
    "기아": "KIA",
    "두산": "Doosan",
    "롯데": "Lotte",
    "삼성": "Samsung",
    "키움": "Kiwoom",
    "한화": "Hanwha",
}

def romanize_korean(text: str) -> str:
    if not text:
        return ""
    
    if text in EXCEPTIONS:
        return EXCEPTIONS[text]
        
    result = ""
    
    cho_keys = list(CHO.keys())
    jung_keys = list(JUNG.keys())
    jong_keys = list(JONG.keys())

    for i, char in enumerate(text):
        code = ord(char)
        if not (0xAC00 <= code <= 0xD7A3):
            result += char
            continue
            
        index = code - 0xAC00
        cho_index = index // 588
        jung_index = (index - (cho_index * 588)) // 28
        jong_index = index % 28

        cho_str = cho_keys[cho_index]
        jung_str = jung_keys[jung_index]
        jong_str = jong_keys[jong_index]

        syl = ""
        
        # First character special rules
        if i == 0:
            if char == "김": syl = "Kim"
            elif char == "이": syl = "Lee"
            elif char == "박": syl = "Park"
            elif char == "최": syl = "Choi"
            elif char == "정": syl = "Jeong"
            elif char == "강": syl = "Kang"
            elif char == "조": syl = "Cho"
            elif char == "윤": syl = "Yoon"
            elif char == "장": syl = "Jang"
            elif char == "임": syl = "Lim"
            else:
                syl = CHO[cho_str] + JUNG[jung_str] + JONG[jong_str]
                
            if syl and syl[0].islower():
                syl = syl[0].upper() + syl[1:]
        else:
            syl = CHO[cho_str] + JUNG[jung_str] + JONG[jong_str]
            if len(text) == 3 and i == 1:
                syl = " " + syl[0].upper() + syl[1:]
            elif len(text) == 3 and i == 2:
                syl = "-" + syl
                
        result += syl
        
    return result

def compact_foreign_player_name(name: str) -> str:
    trimmed = name.strip()
    if " " not in trimmed:
        return trimmed
    parts = [p for p in trimmed.split() if p]
    return parts[-1] if parts else trimmed

def format_player_name(name: str, lang: str = "en") -> str:
    compact_name = compact_foreign_player_name(name)
    if lang == "ko":
        return compact_name
    return romanize_korean(compact_name)

def format_team_name(team: str, lang: str = "en") -> str:
    if lang == "ko":
        return team
    team_exceptions = {
        "두산": "Doosan",
        "롯데": "Lotte",
        "삼성": "Samsung",
        "키움": "Kiwoom",
        "한화": "Hanwha",
        "기아": "KIA",
    }
    return team_exceptions.get(team, team)
