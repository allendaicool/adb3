import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

class AssociationRule
{
	Set<String> leftSide;
	Set<String> rightSide;

	public AssociationRule (Set<String> leftSide, Set<String> rightSide)
	{
		this.leftSide = leftSide;
		this.rightSide = rightSide;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((leftSide == null) ? 0 : leftSide.hashCode());
		result = prime * result
				+ ((rightSide == null) ? 0 : rightSide.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AssociationRule other = (AssociationRule) obj;
		if (leftSide == null) {
			if (other.leftSide != null)
				return false;
		} else if (!leftSide.equals(other.leftSide))
			return false;
		if (rightSide == null) {
			if (other.rightSide != null)
				return false;
		} else if (!rightSide.equals(other.rightSide))
			return false;
		return true;
	}

	double confidence;
	double support;
}

public class ExtractAssociationRule {


	private final static String inputFileName = "leadingDeathCause.csv";
	public static void main(String[] args)
	{
		if (args.length != 3)
		{
			System.out.println("missing input");
			System.exit(-1);
		}
		String inputFileName = args[0];
		double supportValue = Double.valueOf(args[1]);
		double confidenceValue = Double.valueOf(args[2]);
		int totalNumTransaction = 0;

		//eachLineAppearTime
		Map<Integer, Integer> lineAppear = new HashMap<Integer, Integer>();
		//large item set
		Map<Integer, Map<Set<String>, Integer>> mapping  = new HashMap<Integer, Map<Set<String>, Integer>>();
		//each transaction 
		Map<Integer, Set<String>> eachLineInfo = new HashMap<Integer, Set<String>>();

		mapping.put(1, new HashMap<Set<String>, Integer>());

		//totalNumTransaction = calculateOneItemSet(lineAppear, mapping.get(1), eachLineInfo);

		///////////////////////////////////////////////////////////////////////////
		totalNumTransaction = 4;
		lineAppear.put(1, 1);
		lineAppear.put(2, 1);
		lineAppear.put(3, 1);
		lineAppear.put(4, 1);
		Set<String> yihong1 = new HashSet<String>();
		yihong1.add("pen");
		Set<String> yihong2 = new HashSet<String>();
		yihong2.add("ink");
		Set<String> yihong3 = new HashSet<String>();
		yihong3.add("diary");
		Set<String> yihong4 = new HashSet<String>();
		yihong4.add("soap");
		mapping.get(1).put(yihong1, 4);
		mapping.get(1).put(yihong2, 3);
		mapping.get(1).put(yihong3, 3);
		mapping.get(1).put(yihong4, 2);
		Set<String> yihong5 = new HashSet<String>();
		yihong5.add("pen");
		yihong5.add("ink");
		yihong5.add("diary");
		yihong5.add("soap");
		Set<String> yihong6 = new HashSet<String>();
		yihong6.add("pen");
		yihong6.add("ink");
		yihong6.add("diary");
		Set<String> yihong7 = new HashSet<String>();
		yihong7.add("pen");
		yihong7.add("diary");
		Set<String> yihong8 = new HashSet<String>();
		yihong8.add("pen");
		yihong8.add("ink");
		yihong8.add("soap");
		eachLineInfo.put(1, yihong5);
		eachLineInfo.put(2, yihong6);
		eachLineInfo.put(3, yihong7);
		eachLineInfo.put(4, yihong8);
		///////////////////////////////////////////////////////////////////////////

		int largeSetId = 1;
		removeUnQualified(mapping, totalNumTransaction, 1, supportValue);
		while (mapping.get(largeSetId++).size() != 0)
		{
			Set<Set<String>> candidates = aprioriGen(mapping, largeSetId - 1);
			for (Set<String> candidatesTmp : candidates)
			{
				System.out.println("candidate is " + candidatesTmp);
			}
			mapping.put(largeSetId, new HashMap<Set<String>, Integer>());
			Map<Set<String>, Integer> largeItemSetNewRound = mapping.get(largeSetId);


			for (Map.Entry<Integer, Set<String>> entry : eachLineInfo.entrySet())
			{
				Set<Set<String>> subsetCandidates = subSetCandidates(candidates, entry.getValue());
				System.out.println("begining ------------");

				for (Set<String> candidatesTmp : subsetCandidates)
				{
					System.out.println("line number is " + entry.getKey());
					System.out.println("subset candidate is " + candidatesTmp);
				}
				System.out.println("ending ------------");

				for (Set<String> tmpCandidate : subsetCandidates)
				{
					if (!largeItemSetNewRound.containsKey(tmpCandidate))
					{
						largeItemSetNewRound.put(tmpCandidate, lineAppear.get(entry.getKey()));
					}
					else
					{
						largeItemSetNewRound.put(tmpCandidate, lineAppear.get(entry.getKey()) + largeItemSetNewRound.get(tmpCandidate));
					}
				}
			}
			removeUnQualified(mapping, totalNumTransaction, largeSetId, supportValue);
		}
		for (Map.Entry<Integer, Map<Set<String>, Integer>> entryTmp : mapping.entrySet())
		{
			System.out.println("group number is " + entryTmp.getKey());
			for (Map.Entry<Set<String>, Integer> entryTmp2 : entryTmp.getValue().entrySet())
			{
				System.out.println("set string is " + entryTmp2.getKey());
				System.out.println("set string appear time is " + entryTmp2.getValue());
			}
		}
		
		System.out.println("--------------------------------");
		List<AssociationRule> rules = produceAssociationRule(mapping, confidenceValue, totalNumTransaction);
		for (AssociationRule each : rules)
		{
			System.out.println("left side is " + each.leftSide);
			System.out.println("right side is " + each.rightSide);
			System.out.println("confidence value is " + each.confidence);
			System.out.println("support value is " + each.support);
		}
		
	}

	private static List<AssociationRule> produceAssociationRule (Map<Integer, Map<Set<String>, Integer>> mapping,
			double confidenceValue, int totalNumTransaction)
			{
		List<AssociationRule> ret = new ArrayList<AssociationRule>();
		for (Map.Entry<Integer, Map<Set<String>, Integer>> entry : mapping.entrySet())
		{
			if (entry.getKey() == 1)
			{
				continue;
			}
			Map<Set<String>, Integer> largeItemMap = entry.getValue();

			for (Map.Entry<Set<String>, Integer> setString : largeItemMap.entrySet())
			{
				int supportNum = setString.getValue();
				Set<String> calculateSubset = setString.getKey();
				List<AssociationRule> retTmp = produceAllSubset(calculateSubset, supportNum, mapping, confidenceValue,
						supportNum/(double)totalNumTransaction);
				ret.addAll(retTmp);
			}
		}
		return ret;
			}

	private static List<AssociationRule> produceAllSubset (Set<String> calculateSubset, int supporNum,
			Map<Integer, Map<Set<String>, Integer>> mapping, double confidenceBound,  double setSupport)
			{
		int size = calculateSubset.size();
		List<String> subsetTmp = new ArrayList<String>(calculateSubset);
		int count = 1;
		List<AssociationRule> ret = new ArrayList<AssociationRule>();
		while (count < size)
		{
			List<AssociationRule> tmpRet = new ArrayList<AssociationRule>();
			produceAllSubSetHelp(tmpRet, supporNum, new HashSet<String> (), 0, count, 0, 
					subsetTmp, mapping, confidenceBound, setSupport);
			ret.addAll(tmpRet);
			count++;
		}
		return ret;
			}

	private static void produceAllSubSetHelp (List<AssociationRule>ret, int supporNum, Set<String> leftSide,
			int leftNumber, int limit, int ptr, List<String> subSetTmp, Map<Integer, Map<Set<String>, Integer>> mapping, 
			double confidenceBound, double setSupport)
	{
		if (leftNumber == limit)
		{
			double confidence = supporNum/(double)(mapping.get(leftSide.size()).get(leftSide));
			if (confidence < confidenceBound)
			{
				return;
			}
			Set<String> right = new HashSet<String>();
			for (String tmp : subSetTmp)
			{
				if (!leftSide.contains(tmp))
				{
					right.add(tmp);
				}
			}
			AssociationRule newRule = new AssociationRule(new HashSet<String>(leftSide), right);
			newRule.confidence = confidence;
			newRule.support = setSupport;
			ret.add(newRule);
			return;
		}
		for (int i = ptr; i < subSetTmp.size(); i++)
		{
			leftSide.add(subSetTmp.get(i));
			produceAllSubSetHelp(ret, supporNum, leftSide, leftNumber+1, limit, ptr + 1, subSetTmp, mapping, confidenceBound, setSupport);
			leftSide.remove(subSetTmp.get(i));
		}
	}

	private static Set<Set<String>> subSetCandidates(Set<Set<String>> candidates, Set<String> eachTransaction)
	{
		Set<Set<String>> subSet = new HashSet<Set<String>>();
		for (Set<String> entry : candidates)
		{
			boolean containAll = true;
			for (String tmp : entry)
			{
				if (!eachTransaction.contains(tmp))
				{
					containAll = false;
				}
			}
			if (containAll)
			{
				subSet.add(entry);
			}
		}
		return subSet;
	}

	private static Set<Set<String>> aprioriGen (Map<Integer, Map<Set<String>, Integer>> mapping, int round)
	{
		Map<Set<String>, Integer> roundMapping  = mapping.get(round);
		Set<Set<String>> keySet = roundMapping.keySet();
		Set<Set<String>> ret = new HashSet<Set<String>>();
		for (Set<String> entry1 : keySet)
		{
			for (Set<String> entry2 : keySet)
			{
				if (entry1.equals(entry2))
				{
					continue;
				}
				Set<String> newSet = getNewSet(entry1, entry2);
				if (newSet!= null && !ret.contains(newSet))
				{
					ret.add(newSet);
				}
			}
		}
		Iterator<Set<String>> it = ret.iterator();
		while (it.hasNext())
		{
			Set<String> item = it.next();
			List<String> tmpHoler = new ArrayList<String>(item);
			for (int i = 0;i < tmpHoler.size(); i++)
			{
				Set<String> checkItem = new HashSet<String>(item);
				checkItem.remove(tmpHoler.get(i));
				if (!mapping.get(round).containsKey(checkItem))
				{
					it.remove();
				}
			}
		}
		return ret;
	}

	private static Set<String> getNewSet (Set<String> entry1, Set<String> entry2)
	{
		int countDiff = 0;
		String addIn = null;
		Set<String> ret = new HashSet<String>();
		for (String tmp1 : entry1)
		{
			if (entry2.contains(tmp1))
			{
				continue;
			}
			else
			{
				if (countDiff == 1)
				{
					return null;
				}
				countDiff++;
				addIn = tmp1;
			}
		}
		for (String tmp2 : entry2)
		{
			ret.add(tmp2);
		}
		ret.add(addIn);
		return ret;
	}

	private static void removeUnQualified(Map<Integer, Map<Set<String>, Integer>> mapping, int totalNumTransaction, int id, double supportBound)
	{
		Map<Set<String>, Integer> itemSetMapping = mapping.get(id);
		Iterator<Entry<Set<String>, Integer>> it = itemSetMapping.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<Set<String>, Integer> pair = it.next();
			if (((double)pair.getValue()/totalNumTransaction) < supportBound)
			{
				it.remove();
			}
		}
	}
	private static int  calculateOneItemSet(Map<Integer, Integer> lineAppear, Map<Set<String>, Integer> OneItemLargeSet, Map<Integer, Set<String>> eachLineInfo)
	{
		int totalNumTransaction = 0;
		try {
			// FileReader reads text files in the default encoding.
			File csvData = new File(inputFileName);

			// store each line information in the memory
			int count = 1;
			CSVParser parser = CSVParser.parse(csvData,StandardCharsets.US_ASCII, CSVFormat.EXCEL);
			boolean firstRow = true;

			for (CSVRecord csvRecord : parser)
			{
				if (firstRow)
				{
					firstRow = false;
					continue;
				}
				int appearTime = Integer.valueOf(csvRecord.get(4));

				lineAppear.put(count, appearTime);

				totalNumTransaction += appearTime;

				Set<String> eachLineGTerm = new HashSet<String>();
				for (int i = 0; i < 4; i ++)
				{
					Set<String> tmp = new HashSet<String>();
					tmp.add(csvRecord.get(i));
					if (!OneItemLargeSet.containsKey(tmp))
					{
						OneItemLargeSet.put(tmp,appearTime);
					}
					else
					{
						OneItemLargeSet.put(tmp, OneItemLargeSet.get(tmp) + appearTime);
					}
					eachLineGTerm.add(csvRecord.get(i));
				}
				eachLineInfo.put(count++, eachLineGTerm);
			}
			for (Map.Entry<Set<String>, Integer> entry : OneItemLargeSet.entrySet())
			{
				System.out.println("string is " + entry.getKey());
				System.out.println("appear time is " + entry.getValue());
			}

		}
		catch(FileNotFoundException ex) {
			System.out.println(
					"Unable to open file '" + 
							inputFileName + "'");                
		}
		catch(IOException ex) {
			System.out.println(
					"Error reading file '" 
							+ inputFileName + "'");                  
			// Or we could just do this: 
			// ex.printStackTrace();
		}
		return totalNumTransaction;
	}

	private Set<Set<String>> outputLargeItems (int totalNumRecords, double supportValue, Map<Set<String>, Integer> candiates)
	{
		Set<Set<String>> output = new HashSet<Set<String>>();
		for (Map.Entry<Set<String>, Integer> entry : candiates.entrySet())
		{
			if (((double)entry.getValue() / totalNumRecords) > supportValue)
			{
				output.add(entry.getKey());
			}
		}
		return output;
	}
}
