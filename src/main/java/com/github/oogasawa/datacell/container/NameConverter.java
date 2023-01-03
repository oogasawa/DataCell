package com.github.oogasawa.datacell.container;

import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * DataSet名、Predicate名を内部名称に変換したり、逆に内部名称をDataSet名、Predicate名に変換するためのクラス。
 *
 * <p>
 * ColumnarDBは、DataSet名とPredicate名の組みごとに１つのテーブルを作成してデータを管理している。
 * 例えばDataSet名がtaxonで、Predicate名がscientific_nameだとすると、taxon__scientific_nameという名称のテーブルにデータが保持される。
 * </p>
 *
 * <p>
 * しかしバックエンドのDBMSの種類によっては、ユーザーから与えられたDataSet名やPredicate名をテーブル名として使えない場合がある。
 * ユーザーから与えられたDataSet名やPredicate名を"OriginalName"と呼び、
 * DBMSがテーブル名として使う名称を"InternalName"と呼ぶ。
 * このクラスは、OriginalNameとInternalNameの間の変換を行う。
 * </p>
 *
 * <p>
 * 以下の3つのテーブルが利用される。
 * </p>
 * <ul>
 * <li>internal_name__original_name</li>
 * <li>original_name__internal_name</li>
 * <li>internal_name_prefix__max_count</li>
 * </ul>
 * <p>
 * 前二者は、internal name, original nameの対応テーブルである。
 * internal_name_prefix__max_countは、一意にinternal nameをつけるために利用される。
 * いずれにしろgetInternalName()メソッドを使うと自動的にこれらのテーブルに値が保存される。
 * </p>
 *
 *
 *
 * @author Osamu Ogasawara
 *
 */
public class NameConverter {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    DCContainer dbObj = null;

    /**
     * non-whitespace characters
     */
    Pattern nonWS = Pattern.compile("\\W+");

    /**
     * alphabets and numbers
     */
    Pattern alnumPattern = Pattern.compile("[A-Za-z0-9_ ]+");
    Pattern prefixPattern = Pattern.compile("[A-Za-z0-9_]+");

    /**
     * tableNameのパターン ２つの単語をunderscore 2つでつないだ形。
     *
     */
    Pattern tableNamePattern = Pattern.compile("([a-zA-Z0-9_]+?)__([a-zA-Z0-9_]+)");

    /**
     *      */
    final int MAX_LENGTH_OF_PREFIX = 25;
    final int NUMBER_OF_DIGITS = 5;

    /**
     * Default constructor
     */
    public NameConverter() {
    }

    public NameConverter(DCContainer dbObj) {
        this.dbObj = dbObj;
    }

    public void setDatabase(DCContainer dbObj) {
        this.dbObj = dbObj;
    }

    public boolean hasOriginalName(String name) {
        boolean result = false;
        if (dbObj.hasTableInAllTables("INTERNAL_NAME__ORIGINAL_NAME")) {
            ArrayList<String> values = dbObj.getValueList("INTERNAL_NAME__ORIGINAL_NAME", name);
            if (values.size() > 0) {
                result = true;
            }
        }

        return result;
    }

    public String getTableName(String ds, String pred) {
        return getInternalName(ds) + "__" + getInternalName(pred);
    }

    /**
     * OriginalNameに対応するInternalNameを返す。
     *
     * すでにOriginalNameに対応するInternalNameが存在していればデータベース内に保存されているのでそれを検索して返す。
     * まだ存在していなければInternalNameを生成して返し、同時に名前の対応をデータベースに登録する。
     *
     * @param origName - OriginalName
     * @return InternalName
     */
    public String getInternalName(String origName) {

        String internalName = null;

        dbObj.createTableIfAbsent("ORIGINAL_NAME__INTERNAL_NAME");
        ArrayList<String> list = dbObj.getValueList("ORIGINAL_NAME__INTERNAL_NAME", origName);
        if (list.size() >= 1) {
            internalName = list.get(0).toUpperCase();
            if (list.size() > 1) {
                logger.warning("original name " + origName + " has multiple internal names");
            }
        } else if (list.size() == 0) { // not found.
            internalName = makeInternalName(origName);
            setInternalName(origName, internalName.toUpperCase());
        } 

        return internalName.toUpperCase();
    }

    /**
     * OriginalNameに対応するInternalNameを（必要なら生成して）返す。名前がマルチバイト文字を含む時のためにprefixが指定できる.
     *
     * すでにOriginalNameに対応するInternalNameが存在していればデータベース内に保存されているのでそれを検索して返す。
     * まだ存在していなければInternalNameを生成して返し、同時に名前の対応をデータベースに登録する。
     *
     * InternalNameを作成するときに、prefix + 自然数 という形の文字列を生成する。 名前がマルチバイト文字を含む場合に役立つ。
     *
     * @param origName - OriginalName
     * @param prefix   InternalNameを作成するときに使用するprefix
     * @return InternalName
     */
    public String getInternalName(String origName, String prefix) {

        String internalName = null;

        dbObj.createTableIfAbsent("ORIGINAL_NAME__INTERNAL_NAME");
        ArrayList<String> list = dbObj.getValueList("ORIGINAL_NAME__INTERNAL_NAME", origName);
        if (list.size() >= 1) {
            internalName = list.get(0).toUpperCase();
            if (list.size() > 1) {
                logger.warning("original name " + origName + " has multiple internal names");
            }
        } else if (list.size() == 0) { // not found.
            internalName = makeInternalName(origName, prefix);
            setInternalName(origName, internalName.toUpperCase());
        }

        return internalName.toUpperCase();
    }

    /**
     * InternalNameに対応するOriginalNameを返す。
     *
     * すでにOriginalNameとInternalNameの対応がとれている場合、対応がデータベース中に保存されているのでデータベースを検索してOriginalNameを返す。
     * InternalNameに対応するOriginalNameがない場合はnullが返される。
     *
     * @param intName InternalName
     * @return OriginalName. If it does not exist, null is returned.
     */
    public String getOriginalName(String intName) {
        String origName = null;

        dbObj.createTableIfAbsent("INTERNAL_NAME__ORIGINAL_NAME");
        ArrayList<String> values = dbObj.getValueList("INTERNAL_NAME__ORIGINAL_NAME", intName.toUpperCase());

        if (values.size() == 0) { // not found.
            // nothing to do.
        } else if (values.size() >= 1) {
            origName = values.get(0);
            if (values.size() > 1) {
                logger.warning("invalid number of original name to " + intName.toUpperCase());
            }

        } 

        return origName;
    }

    /**
     * OriginalNameに対応するInternalName文字列を新規に作成する.
     *
     * <ul>
     * <li>OriginalNameが25文字以内のアルファベット、数字、アンダースコアからなる場合は、OriginalNameとInternalNameは同じ文字列になる。
     * <li>OriginalNameが25文字以内のアルファベット、数字、アンダースコアおよびwhitespaceから成る場合は、whitespaceはアンダースコアに置換される。
     * <li>throw new RuntimeException("Unexpected Error : invalid number of original
     * name to " + intName);
     * </ul>
     *
     * @param origName OriginalName
     * @return InternalName
     */
    public String makeInternalName(String origName) {

        String internalName = null;
        String prefix = null;
        int counter = 0;

        Matcher m = alnumPattern.matcher(origName);
        if (m.matches()) {

            if (origName.length() <= MAX_LENGTH_OF_PREFIX) {
                internalName = origName.replace(" ", "_");
            } else { // origName.length > MAX_LENGTH_OF_PREFIX
                prefix = origName.substring(0, MAX_LENGTH_OF_PREFIX);
                prefix = prefix.replace(" ", "_");
                counter = getCount(prefix);
                internalName = prefix + String.format("%0" + NUMBER_OF_DIGITS + "d", ++counter);
                setCount(prefix, counter);
            }

        } else { // original name contains non alpha numerical characters.
            internalName = makeInternalName(origName, "NONALNUM");
        }

        return internalName.toUpperCase();
    }

    /**
     * OriginalNameに対応するInternalName文字列を新規に作成する.
     *
     * <ul>
     * <li>OriginalNameが25文字以内のアルファベット、数字、アンダースコアからなる場合は、OriginalNameとInternalNameは同じ文字列になる。
     * <li>OriginalNameが25文字以内のアルファベット、数字、アンダースコアおよびwhitespaceから成る場合は、whitespaceはアンダースコアに置換される。
     * <li>
     * </ul>
     *
     * @param origName OriginalName
     * @param prefix   InternalName作成のときに使用するprefix
     * @return InternalName
     * @excetion RuntimeException Original name contains non alpha numerical characters
     */
    public String makeInternalName(String origName, String prefix) {

        String internalName = null;
        int counter = 0;

        Matcher m = prefixPattern.matcher(prefix);
        if (m.matches() && prefix.length() <= MAX_LENGTH_OF_PREFIX) {
            counter = getCount(prefix);
            internalName = prefix + String.format("%0" + NUMBER_OF_DIGITS + "d", ++counter);
            setCount(prefix, counter);
        } else { 
            logger.finer("Given prefix string is invalid.");
            throw new RuntimeException("Given prefix string is invalid. (" + origName + ", " + prefix + ")");
        }

        return internalName.toUpperCase();
    }

    
    public String makeTableName(String dataset, String predicate) {
        String subject0 = getInternalName(dataset);
        String predicate0 = getInternalName(predicate);
        return subject0 + "__" + predicate0;
    }

    
    /**
     * table nameを受け取って、datasetとpredicateのinternal nameを返す。
     *
     * @param tableName テーブル名
     * @return tableName から抽出された dataset と predicate
     */
    public ArrayList<String> parseTableName(String tableName) {
        ArrayList<String> result = new ArrayList<String>();
        Matcher m = tableNamePattern.matcher(tableName);
        if (m.matches()) {
            result.add(m.group(1).toUpperCase());
            result.add(m.group(2).toUpperCase());
        }
        return result;
    }

    public boolean isValidTableName(String tableName) {
        Matcher m = tableNamePattern.matcher(tableName);
        return m.matches();
    }

    public int getCount(String prefix) {
        int count = 0;
        ArrayList<String> cList = dbObj.getValueList("INTERNAL_NAME_PREFIX", prefix, "MAX_COUNT");
        if (cList.size() > 0) {
            count = Integer.valueOf(cList.get(0));
        }

        return count;
    }

    public void setCount(String prefix, int count) {
        dbObj.putRowWithReplacingValues("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix, String.format("%d", count));
    }

    /**
     * Sets an original name - internal name pair to the management table.
     *
     * This method sets an original name - internal name correspondence data to the
     * management table, that is INTERNAL_NAME__ORIGINAL_NAME and
     * ORIGINAL_NAME__INTERNAL_NAME.
     *
     * @param origName     OriginalName
     * @param internalName InternalName
     */
    public void setInternalName(String origName, String internalName) {
        dbObj.putRowIfKeyIsAbsent("INTERNAL_NAME__ORIGINAL_NAME", internalName.toUpperCase(), origName);
        dbObj.putRowIfKeyIsAbsent("ORIGINAL_NAME__INTERNAL_NAME", origName, internalName.toUpperCase());

        /*
         * if (!dbObj.hasTable("original_name__internal_name")) {
         * dbObj.putRowOverwriting("internal_name__original_name", internalName,
         * origName); dbObj.putRowOverwriting("original_name__internal_name", origName,
         * internalName); } else if (dbObj.hasKey("original_name__internal_name",
         * origName)) { return; } else { dbObj.putRow("internal_name__original_name",
         * internalName, origName); dbObj.putRow("original_name__internal_name",
         * origName, internalName); }
         */
    }

}
