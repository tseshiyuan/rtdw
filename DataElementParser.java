package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

/**
 * Created by chiyao on 10/4/14.
 */
public class DataElementParser {

    public enum TokenType {
        SYMBOL,NUMBER,DATE,COLON,LEFT_BRACKET,RIGHT_BRACKET,LEFT_CURL,RIGHT_CURL
    };

    public static class Token {
        TokenType type;
        String value;
        Token(TokenType type, String value) {
            this.type = type;
            this.value=value;
        }
    }

    public static Token[] lexer(String input) {
        ArrayList<Token> al =  new ArrayList<Token>();
        String[] parts = input.split(",");
        for (String s: parts) {
            addParts(s.trim().toCharArray(),0,al);
        }
        Token[] result = new Token[al.size()];
        System.arraycopy(al.toArray(),0,result,0,result.length);
        return result;
    }

    private static final void addParts(char[] part, int startPos, ArrayList<Token> result) {
        if (startPos==part.length) {
            return;
        }
        switch(part[startPos]) {
            case '[':
                result.add(new Token(TokenType.LEFT_BRACKET,null));
                addParts(part,startPos+1,result);
                return;
            case ']':
                result.add(new Token(TokenType.RIGHT_BRACKET,null));
                addParts(part,startPos+1,result);
                return;
            case '{':
                result.add(new Token(TokenType.LEFT_CURL,null));
                addParts(part,startPos+1,result);
                return;
            case '}':
                result.add(new Token(TokenType.RIGHT_CURL,null));
                addParts(part,startPos+1,result);
                return;
            case ':':
                result.add(new Token(TokenType.COLON,null));
                addParts(part,startPos+1,result);
                return;
            case '^': //datetime
                result.add(new Token(TokenType.DATE,new String(part,startPos+1,10))); // epoch is 10 digits (to seconds)
                addParts(part,startPos+11,result);
                return;
        }
        if (part[startPos] >= '0' && part[startPos] <= '9') { // number
            int i;
            for (i=startPos; i<part.length && (part[i] >= '0' && part[i] <= '9' || part[i]=='.' || part[i]=='E'); i++) {}
            result.add(new Token(TokenType.NUMBER,new String(part,startPos,i-startPos)));
            addParts(part,i,result);
        }
        else {  // symbol
            int i;
            for (i=startPos; i<part.length && part[i] != '[' && part[i] != ']' && part[i] != '{' && part[i] != '}' && part[i] != ':'; i++) {}
            result.add(new Token(TokenType.SYMBOL,new String(part,startPos,i-startPos)));
            addParts(part,i,result);
        }
    }


    public static final DataElement parse(String s) {
        Token[] tokens = lexer(s);
        DataElement result = new DataElement(DataType.NUMBER,0); // result place holder
        int pos = parse(tokens,0, result);
        if (pos == tokens.length) {// succeed
            return result;
        }
        else {
            return null;
        }
    }

    /**
     * parse 3 cases and return a new position if any succeeds
     * @param input
     * @param startPos
     * @param result            result parsed tree
     * @return
     */
    public static final int parse(Token[] input, int startPos, DataElement result) {

        int endPos = parseSequence(input, startPos, result);
        if (endPos != -1) {
            return endPos;
        }
        endPos = parseMap(input, startPos,result);
        if (endPos != -1) {
            return endPos;
        }
        endPos = parseIdentifier(input, startPos, result);
        if (endPos != -1) {
            return endPos;
        }
        return -1;
    }


    public static final int parseIdentifier(Token[] input, int startPos, DataElement result) {
        if (startPos >= input.length) {
            return -1;
        }
        Token token = input[startPos];
        if (token.type == TokenType.SYMBOL) {
            result.setToText(token.value);
        }
        else if (token.type == TokenType.NUMBER) {
            result.setToNumber(Float.valueOf(token.value));
        }
        else if (token.type == TokenType.DATE) {
            result.setToDateTime(new Date(Long.valueOf(token.value)*1000)); // convert ms to sec
        }
        return startPos+1;
    }


    /**
     *
     * @param input
     * @param startPos
     * @param result   parse tree
     * @return new startPos after successful parsing
     */
    public static final int parseSequence(Token[] input, int startPos,DataElement result) {
        if (startPos >= input.length || input[startPos].type != TokenType.LEFT_BRACKET) {
            return -1;
        }
        startPos++;
        ArrayList<DataElement> al = new ArrayList<DataElement>();
        while (startPos>=0 && startPos < input.length && input[startPos].type != TokenType.RIGHT_BRACKET) {
            DataElement elt = new DataElement(DataType.NUMBER,0); // just a place holder
            al.add(elt);
            startPos = parse(input,startPos,elt);
        }
        if (startPos < input.length && input[startPos].type == TokenType.RIGHT_BRACKET) { // succeed
            result.setToList(al);
            return startPos+1;
        }
        else {
            return -1;
        }
    }


    public static final int parseMap(Token[] input, int startPos, DataElement result) {
        if (startPos >= input.length || input[startPos].type != TokenType.LEFT_CURL) {
            return -1;
        }
        startPos++;
        TreeMap<String,DataElement> tm = new TreeMap<String,DataElement>();
        while (startPos < input.length && input[startPos].type != TokenType.RIGHT_CURL) {
            Pair pair = new Pair();
            startPos = parsePair(input, startPos, pair);
            if (startPos<0) {
                return -1;
            }
            tm.put(pair.name, pair.value);
        }
        if (startPos < input.length && input[startPos].type == TokenType.RIGHT_CURL) { // succeed
            result.setToMap(tm);
            return startPos+1;
        }
        else {
            return -1;
        }
    }

    /**
     * parse a pair String:DataElement
     * @param input
     * @param startPos
     * @param result
     * @return
     */
    public static final int parsePair(Token[] input, int startPos, Pair result) {
        if (startPos+2 >= input.length) {
            return -1; // failed
        }
        if (input[startPos].type != TokenType.SYMBOL) {
            return -1; // failed
        }
        if (input[startPos+1].type != TokenType.COLON) {
            return -1; // failed
        }
        DataElement elt = new DataElement(DataType.NUMBER,0); // place holder
        int pos = parse(input,startPos+2, elt);
        if (pos >= 0) {
            result.name = input[startPos].value;
            result.value = elt;
        }
        return pos;
    }


    // utility class
    private static class Pair {
        String name;
        DataElement value;

        Pair() {}
        void setName(String n) {name=n;}
        void setValue(DataElement v) {value=v;}
    }


    public static final void main(String[] args) {
        try {

            //String s= "[a,[1,2],d]";
            //String s = "{a:1,b:[2,3],[x,{i:1,j:2}]}";
            String s = "{a:^1412520517,b:[1412520517,1.41252058E9],c:[x,{i:1,j:2}]}";
            DataElement e = parse(s);
            System.out.println(e);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
