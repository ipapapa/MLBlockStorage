package edu.purdue.simulation.blockstorage;

import java.util.*;

import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;

public class PerformanceCalculator {
	
	public PerformanceCalculator(){
		this.VolumeCalculatedSpecifications = new HashMap<Volume, VolumeSpecifications>();
	}
	
	private Map<Volume, VolumeSpecifications> VolumeCalculatedSpecifications;
	
	public boolean addVolume(Volume Volume){
		if(this.VolumeCalculatedSpecifications.containsKey(Volume))
			
			return false;
		
		this.VolumeCalculatedSpecifications.put(Volume, null);
		
		return true;
	}
	
	public void CollectVolumesSpecifications(){
		
	}
	
	public BackEndSpecifications CalculateBackEndSpecifications(Backend backEnd){
		return null;
	}
	
	
}
