package cn.ioceye.core.sorted;

import java.io.Serializable;

import scala.math.Ordered;

public class SencondarySortKey implements Ordered<SencondarySortKey>, Serializable{
	private static final long serialVersionUID = 1L;
	
	private int first;
	private int second;
	
	public SencondarySortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean $greater(SencondarySortKey other) {
		if(first > other.first) {
			return true;
		}
		else if(first == other.first && second > other.second) {
			return true;
		}
		else {
			return false;	
		}
	}

	@Override
	public boolean $greater$eq(SencondarySortKey other) {
		if($greater(other)) {
			return true;
		}
		else if(first == other.first && second == other.second) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SencondarySortKey other) {
		if(first < other.first) {
			return true;
		}
		else if(first == other.first && second < other.second) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SencondarySortKey other) {
		if($less(other)) {
			return true;
		}
		else if(first == other.first && second == other.second){
			return true;
		}
		return false;
	}

	@Override
	public int compare(SencondarySortKey other) {
		int firstDis  = first - other.first;
		int secondDis = second - other.second;
		if(firstDis != 0) {
			return firstDis;
		}
		else  {
			return secondDis;
		}
	}

	@Override
	public int compareTo(SencondarySortKey other) {
		int firstDis  = first - other.first;
		int secondDis = second - other.second;
		if(firstDis != 0) {
			return firstDis;
		}
		else  {
			return secondDis;
		}
	}

	public int getFirst() {
		return first;
	}
	public void setFirst(int first) {
		this.first = first;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
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
		SencondarySortKey other = (SencondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
}
