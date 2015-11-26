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

		//large item set
		Map<Integer, Map<Set<String>, Integer>> mapping  = new HashMap<Integer, Map<Set<String>, Integer>>();
		//each transaction 
		Map<Integer, Set<String>> eachLineInfo = new HashMap<Integer, Set<String>>();
		mapping.put(1, new HashMap<Set<String>, Integer>());
		totalNumTransaction = calculateOneItemSet(mapping.get(1), eachLineInfo);
		int largeSetId = 1;
		removeUnQualified(mapping, totalNumTransaction, 1, supportValue);
		while (mapping.containsKey(largeSetId++))
		{
			Set<Set<String>> candidates = aprioriGen(mapping, largeSetId - 1);
			mapping.put(largeSetId, new HashMap<Set<String>, Integer>());
			Map<Set<String>, Integer> largeItemSetNewRound = mapping.get(largeSetId);
	        for (Map.Entry<Integer, Set<String>> entry : eachLineInfo.entrySet())
	        {
	        	Set<Set<String>> subsetCandidates = subSetCandidates(candidates, entry.getValue());
	        	for (Set<String> tmpCandidate : subsetCandidates)
	        	{
	        		if (!largeItemSetNewRound.containsKey(tmpCandidate))
	        		{
	        			largeItemSetNewRound.put(tmpCandidate, entry.getKey());
	        		}
	        		else
	        		{
	        			largeItemSetNewRound.put(tmpCandidate, entry.getKey() + largeItemSetNewRound.get(tmpCandidate));
	        		}
	        	}
	        }
	        removeUnQualified(mapping, totalNumTransaction, largeSetId, supportValue);
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
		for (Set<String> entry1 : keySet)
		{
			for (Set<String> entry2 : keySet)
			{
				if (entry1.equals(entry2))
				{
					continue;
				}
				Set<String> newSet = getNewSet(entry1, entry2);
				if (newSet!= null && !keySet.contains(newSet))
				{
					keySet.add(newSet);
				}
			}
		}
		Iterator<Set<String>> it = keySet.iterator();
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
		return keySet;
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
				if (countDiff == 2)
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
	private static int  calculateOneItemSet(Map<Set<String>, Integer> OneItemLargeSet, Map<Integer, Set<String>> eachLineInfo)
	{
		int totalNumTransaction = 0;
		try {
			// FileReader reads text files in the default encoding.
			File csvData = new File(inputFileName);

			// store each line information in the memory
			int count = 0;
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
