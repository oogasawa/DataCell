package net.ogalab.datacell.util;

import org.apache.commons.configuration.ConfigurationException;

import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.util.fundamental.Type;


/** DBWriterの動作確認プログラム。
 * 
 * <h3>Example</h3>
 * <pre>{@code
 * 	public static void main(String[] args) {
 *		CUIExample obj = new CUIExample();
 *		try {
 *			obj.makeExampleDB();
 *		} catch (ConfigurationException e) {
 *			e.printStackTrace();
 *		}
 *	}
 * }</pre>
 * 
 * 
 * @author oogasawa
 *
 */
public class CUIExample {
	
	protected DCContainerFactory facObj = null;
	
	String[] testData = {
		"秀逸な記事とは、秀逸な記事の選考を通過した記事のことです。選考を通過した記事は、執筆者の努力を顕彰し、他の記事執筆者へ執筆の参考例を示すために、このリストに追加され、メインページにローテーションで紹介されます。",
		"このリストにある記事は、ウィキペディア日本語版の記事の中でも高品質なものです。あなたがよく書けている記事を他にご存じなら、また、ご自分で素晴らしい記事が書けたと考えたなら、秀逸な記事に推薦してください。",
		"秀逸な記事は「記事対象そのもの」の優劣や善悪によって選出されているわけではありません。選考される際の要点は、あくまで百科事典の記事としての「質」・「量」・「書式」に問題がないかどうかです。詳しくは「Wikipedia:秀逸な記事の選考」をご覧ください。",
		"現在、ウィキペディア日本語版全体で 851,617 本の記事があり、そのうち 67 本の記事が秀逸な記事に選ばれています。記事の分類は、日本十進分類法の2次区分までを基本としています。2次区分以下が5項目以上になった場合は、3次区分を使用しています。"	
	};
	
	
	public CUIExample(DCContainerFactory facObj) {
		this.facObj = facObj;
	}


	public void makeExampleDB() throws ConfigurationException {
		DCContainer dbObj = facObj.getInstance("example_db");

		for (int i=0; i<10000000; i++) {
			String id = Type.toString(i);
			String value = testData[i%4];
			dbObj.putRowIfKeyValuePairIsAbsent("example", id, "value", value);
			if (i%100000 == 0)
				System.out.println("");
			if (i%10000 == 0)
				System.out.print(".");
			
		}
		dbObj.close();
	}
	
	
}
