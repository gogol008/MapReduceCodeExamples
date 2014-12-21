package com.deb.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XMLParser {

	private Document doc;

	public XMLParser(String fXmlFile) throws SAXException, IOException, ParserConfigurationException {

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(fXmlFile));

		doc = builder.parse(is);
	}

	public long getUsercount() {
		NodeList nList = doc.getElementsByTagName("usercount");
		return Long.parseLong(nList.item(0).getTextContent());

	}

	@SuppressWarnings("null")
	public List<String> getCategory() {
		List<String> categoriesList = new ArrayList<String>();
		NodeList nList = doc.getElementsByTagName("category");
		String categories = nList.item(0).getTextContent();
		StringTokenizer st = new StringTokenizer(categories, "/");
		while (st.hasMoreElements()) {
			String eachCat = st.nextToken();
			if (eachCat != null || !eachCat.isEmpty()) {
				categoriesList.add(eachCat.toLowerCase());
			}
		}

		return categoriesList;

	}

	public String getUrl() {
		NodeList nList = doc.getElementsByTagName("url");
		return nList.item(0).getTextContent();
	}

	public String getHash() {
		NodeList nList = doc.getElementsByTagName("hash");
		return nList.item(0).getTextContent();
	}

	public List<String> getReviews() {
		List<String> returnStringList = new ArrayList<String>();
		NodeList nList = doc.getElementsByTagName("review");
		for (int index = 0; index < nList.getLength(); index++) {
			returnStringList.add(nList.item(index).getTextContent());
			// System.out.println(" reviews " +
			// nList.item(index).getTextContent());
		}
		return returnStringList;
	}


}
