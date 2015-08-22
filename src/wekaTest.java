import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Random;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.REPTree;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class wekaTest {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		String arguments = "-t D:\\SAS\\2\\514Cat2.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c 3";

		weka.classifiers.trees.REPTree repTree = new REPTree();

		AbstractClassifier.runClassifier(repTree, arguments.split(" "));

		Instance instance = new DenseInstance(4);

		instance.setValue(0, 3.2); // clock
		instance.setValue(1, 4); // num
		//instance.setValue(2, 95); //vio
		instance.setValue(3, 1400); // total

		System.out.println(Arrays.toString(repTree
				.distributionForInstance(instance)));

		if (false) {

			// Declare two numeric attributes
			Attribute at1 = new Attribute("clock");
			Attribute at2 = new Attribute("num");
			Attribute at3 = new Attribute("nextIOPS");

			FastVector<String> fvClassVal = new FastVector<String>(2);

			fvClassVal.addElement("v1");
			fvClassVal.addElement("v2");
			fvClassVal.addElement("v3");
			fvClassVal.addElement("v4");

			Attribute at4 = new Attribute("vio", fvClassVal);

			Attribute at5 = new Attribute("tot");

			FastVector<Attribute> fvWekaAttributes = new FastVector<Attribute>(
					5);

			fvWekaAttributes.addElement(at1);
			fvWekaAttributes.addElement(at2);
			fvWekaAttributes.addElement(at3);
			fvWekaAttributes.addElement(at4);
			fvWekaAttributes.addElement(at5);

			Instances isTrainingSet = new Instances("Rel", fvWekaAttributes, 10);

			isTrainingSet.setClassIndex(4);

			// Create the instance
			Instance iExample = new DenseInstance(5);
			iExample.setValue((Attribute) fvWekaAttributes.elementAt(0), 12.5);
			iExample.setValue((Attribute) fvWekaAttributes.elementAt(1), 4);
			iExample.setValue((Attribute) fvWekaAttributes.elementAt(2), 95);
			iExample.setValue((Attribute) fvWekaAttributes.elementAt(3), "v1");
			iExample.setValue((Attribute) fvWekaAttributes.elementAt(4), 800);

			// add the instance
			isTrainingSet.add(iExample);

			
			
			System.out.println(Arrays.toString(repTree
					.distributionForInstance(iExample)));

			// repTree.distributionForInstance(instance)

			// double l = repTree.classifyInstance(instance);

			BufferedReader breader = new BufferedReader(new FileReader(
					"D:\\SAS\\2\\514Cat.arff"));

			Instances train = new Instances(breader);

			train.setClassIndex(train.numAttributes() - 1);

			breader.close();

			instance.setDataset(train);

			instance.setValue(0, 1);
			instance.setValue(1, 1);
			instance.setValue(2, 300);
			instance.setValue(3, 0);
			instance.setValue(4, 300);
			//
			//
			//
			// "-t D:\\SAS\\2\\514Cat.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c 4"
			repTree.buildClassifier(train);

			Evaluation eval = new Evaluation(train);

			eval.crossValidateModel(repTree, train, 10, new Random(1));

			System.out.println(eval.toSummaryString("\nResults\n=======\n",
					true));

			System.out.println(eval.fMeasure(1) + " " + eval.precision(1) + " "
					+ eval.recall(1));

		}
	}
}
