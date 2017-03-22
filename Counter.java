enum Counter {

    sentences {
        int apply(String s) {

            int ln = (s != null)? s.length() : 0;
            if ( ln > 0 ) {
                char c = s.charAt(ln - 1);
                return c == '.' || c == '!' || c == '?' ? 1 : 0;
            }else
                return  0;
        }
    },
    words {
        int apply(String s) {
            return (s == null || s.isEmpty()|| !Character.isLetter

(s.charAt(0)))? 0 : 1;
        }
    },
    lattes {
        int apply(String s) {
            int i = 0;
            if (s  != null && !s.isEmpty())
            for (char c : s.trim().toCharArray()) if  

(Character.isLetter(c)) i++;
            return i;
        }
    };

    abstract int apply(String s);

}
