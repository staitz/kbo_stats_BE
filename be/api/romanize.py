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

# Hardcoded KBO player name exceptions (foreign players sourced from DB)
EXCEPTIONS = {
    # Korean players (irregular romanization)
    "김도영": "Kim Do-yeong",
    "구자욱": "Koo Ja-wook",
    "최정": "Choi Jeong",
    "이정후": "Lee Jung-hoo",
    "류현진": "Ryu Hyun-jin",
    "양의지": "Yang Eui-ji",
    "박병호": "Park Byung-ho",
    "구본혁": "Koo Bon-hyeok",
    "강백호": "Kang Baek-ho",
    # Foreign players (all confirmed in DB)
    "가라비토": "Garabito",
    "감보아": "Gamboa",
    "네일": "Neil",
    "데이비슨": "Davidson",
    "디아즈": "Diaz",
    "라일리": "Riley",
    "레예스": "Reyes",
    "레이예스": "Reyes",
    "로건": "Logan",
    "로젠버그": "Rosenberg",
    "로하스": "Rojas",
    "리베라토": "Liberato",
    "맥브룸": "McBroom",
    "메르세데스": "Mercedes",
    "반즈": "Barnes",
    "벨라스케즈": "Velasquez",
    "소크라테스": "Socrates",
    "스톤": "Stone",
    "스티븐슨": "Stevenson",
    "알칸타라": "Alcantara",
    "앤더슨": "Anderson",
    "에레디아": "Heredia",
    "에르난데스": "Hernandez",
    "오스틴": "Austin",
    "올러": "Oller",
    "와이스": "Weiss",
    "웰스": "Wells",
    "위즈덤": "Wisdom",
    "잭로그": "Logue",
    "치리노스": "Chirinos",
    "카디네스": "Cardines",
    "케이브": "Cave",
    "윈": "Wijn",         # 코엔 윈 → compact → 윈
    "콜어빈": "Irvin",
    "쿠에바스": "Cuevas",
    "톨허스트": "Tolhurst",
    "패트릭": "Patrick",
    "페라자": "Peraza",
    "도슨": "Dawson",
    "폰세": "Ponce",
    "푸이그": "Puig",
    "플로리얼": "Florial",
    "헤이수스": "Jesus",
    "화이트": "White",
    "후라도": "Jurado",
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
