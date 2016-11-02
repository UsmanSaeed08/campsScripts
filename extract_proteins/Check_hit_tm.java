package extract_proteins;


import java.io.ByteArrayInputStream;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import mips.gsf.de.simapfeatureclient.client.FeatureClient;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Check_hit_tm {

	public boolean phobius = false;

	public int start_[] = new int[1000];
	public int end_[] = new int[1000];
	int tmsCount = 0;

	public boolean run(String md5) { // seq is the hit sequence for which it has to be checked if the tm exists?

		boolean p = false;

		FeatureClient f = null;
		//SimapAccessWebService simap = null;
		Document doc = null;

		try {
			f = new FeatureClient("http://ws.csb.univie.ac.at/simapwebservice/services/SimapService");
			//simap = new SimapAccessWebService();
			//String md5 = simap.computeMD5(seq);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(new ByteArrayInputStream(f.getFeaturesXML(md5).getBytes()));



			NodeList featureNodes = doc.getElementsByTagName("feature");
			for (int i = 0; i < featureNodes.getLength(); i++) {
				Node featureNode = featureNodes.item(i);
				NamedNodeMap featureNodeAttributes = featureNode.getAttributes();
				phobius = false;
				for (int j = 0; j < featureNodeAttributes.getLength(); j++) {	//check if phobius prediction
					Node featureNodeAttribute = featureNodeAttributes.item(j);

					if (featureNodeAttribute.getNodeName().equals("modelid") && featureNodeAttribute.getNodeValue().equals("PHOBIUS_TM")) {
						phobius = true;
						p = true;
						break;
						//System.out.print(f.getFeaturesXML(md5));
					}
				}


				if (phobius) {	//only if phobius prediction exists for the hit
					NodeList locationNodes = featureNode.getChildNodes();

					int start = -1;

					int z =-1;
					int end = -1;
					for (int j = 0; j < locationNodes.getLength(); j++) {
						Node locationNode = locationNodes.item(j);
						if (locationNode.getNodeName().equals("location")) {
							tmsCount++;
							z++;
							NodeList posNodes = locationNode.getChildNodes();
							for (int k = 0; k < posNodes.getLength(); k++) {
								Node posNode = posNodes.item(k);
								if (posNode.getNodeName().equals("begin")) {
									NamedNodeMap beginNodeAttributes = posNode.getAttributes();
									for (int l = 0; l < beginNodeAttributes.getLength(); l++) {
										Node beginNodeAttribute = beginNodeAttributes.item(l);
										if (beginNodeAttribute.getNodeName().equals("position")) {
											//											System.out.println("begin: " + beginNodeAttribute.getNodeValue());
											start = Integer.parseInt(beginNodeAttribute.getNodeValue()) - 1;
											start_[z] = start;
										}
									}
								}
								if (posNode.getNodeName().equals("end")) {
									NamedNodeMap endNodeAttributes = posNode.getAttributes();
									for (int l = 0; l < endNodeAttributes.getLength(); l++) {
										Node endNodeAttribute = endNodeAttributes.item(l);
										if (endNodeAttribute.getNodeName().equals("position")) {
											//											System.out.println("end: " + endNodeAttribute.getNodeValue());
											end = Integer.parseInt(endNodeAttribute.getNodeValue()) - 1;
											end_[z] = end;
										}
									}
								}
							}
						}
					}
					int length = end - start + 1;

				}


			}


		} catch (Exception ex) {
			ex.printStackTrace();

		}

		return p;


	}


}
