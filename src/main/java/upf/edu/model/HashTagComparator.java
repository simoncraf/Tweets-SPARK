package upf.edu.model;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
public class HashTagComparator implements Comparator{

	@Override
	public int compare(Object arg0, Object arg1) {
        HashTagCount h1=(HashTagCount)arg0;
        HashTagCount h2=(HashTagCount)arg1;

        return h2.count.compareTo(h1.count);
	}

}