/**
 *     PureEdgeSim:  A Simulation Framework for Performance Evaluation of Cloud, Edge and Mist Computing Environments 
 *
 *     This file is part of PureEdgeSim Project.
 *
 *     PureEdgeSim is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     PureEdgeSim is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with PureEdgeSim. If not, see <http://www.gnu.org/licenses/>.
 *     
 *     @author Charafeddine Mechalikh
 **/
package com.mechalikh.pureedgesim.taskgenerator;

import java.util.ArrayList;
import java.util.List;

import com.mechalikh.pureedgesim.datacentersmanager.ComputingNode;

public class ParentTask extends TaskAbstract {
	protected double offloadingTime;
	protected ComputingNode device = ComputingNode.NULL;
	protected long containerSize; // in bits
	protected ComputingNode registry = ComputingNode.NULL;
	protected int applicationID;
	protected FailureReason failureReason;
	protected Status status = Status.SUCCESS;
	protected long fileSize; // in bits
	protected ComputingNode computingNode = ComputingNode.NULL;
	protected double outputSize; // in bits
	protected String type;
	protected ComputingNode orchestrator = ComputingNode.NULL;
	List<SubTask> subtasks = new ArrayList<SubTask>();

	public ParentTask(int id) {
		super(id);
	}

	@Override
	public void setTime(double time) {
		this.offloadingTime = time;
	}

	@Override
	public double getTime() {
		return offloadingTime;
	}

	@Override
	public ComputingNode getEdgeDevice() {
		return device;
	}

	@Override
	public void setEdgeDevice(ComputingNode device) {
		this.device = device;
	}

	@Override
	public void setContainerSizeInBits(long containerSize) {
		this.containerSize = containerSize;
	}

	@Override
	public long getContainerSizeInBits() {
		return containerSize;
	}

	@Override
	public double getContainerSizeInMBytes() {
		return containerSize / 8000000.0;
	}

	@Override
	public ComputingNode getOrchestrator() {
		if (this.orchestrator == ComputingNode.NULL) {
			this.getEdgeDevice().setAsOrchestrator(true);
			return this.getEdgeDevice();
		}
		return this.orchestrator;
	}

	@Override
	public void setOrchestrator(ComputingNode orchestrator) {
		this.orchestrator = orchestrator;
	}

	@Override
	public ComputingNode getRegistry() {
		return registry;
	}

	@Override
	public void setRegistry(ComputingNode registry) {
		this.registry = registry;
	}

	@Override
	public int getApplicationID() {
		return applicationID;
	}

	@Override
	public void setApplicationID(int applicationID) {
		this.applicationID = applicationID;
	}

	@Override
	public FailureReason getFailureReason() {
		return failureReason;
	}

	@Override
	public void setFailureReason(FailureReason reason) {
		this.setStatus(Task.Status.FAILED);
		this.failureReason = reason;
	}

	@Override
	public ComputingNode getOffloadingDestination() {
		return computingNode;
	}

	@Override
	public void setOffloadingDestination(ComputingNode applicationPlacementLocation) {
		this.computingNode = applicationPlacementLocation;
	}

	@Override
	public ParentTask setFileSizeInBits(long requestSize) {
		this.fileSize = requestSize;
		return this;
	}

	@Override
	public ParentTask setOutputSizeInBits(long outputSize) {
		this.outputSize = outputSize;
		return this;
	}

	@Override
	public double getFileSizeInBits() {
		return fileSize;
	}

	@Override
	public double getOutputSizeInBits() {
		return this.outputSize;
	}

	@Override
	public void setStatus(Status status) {
		this.status = status;
	}

	@Override
	public Status getStatus() {
		return status;
	}
	
	@Override
	public String getType() {
		return type;
	}

	@Override
	public void setType(String type) {
		this.type = type;
	}

	public List<SubTask> getSubTasks() {
		return this.subtasks;
	}

	public void defineSubTasksRequirements() {
		for(int i=0; i< this.subtasks.size();i++) { //browse subtasks
			for(int j=0; j<this.subtasks.get(i).getRequirements().length;j++) { //browse there requirements
				if(this.subtasks.get(i).getRequirements()[j].equals("")) continue;
				getSubTaskById(this.subtasks.get(i).getRequirements()[j]).addDestination(this.subtasks.get(i)); //direct the execution results to this task
			}
		}
			
	}

	private SubTask getSubTaskById(String id) {
		for(int i=0; i< this.subtasks.size();i++) {
			if (this.subtasks.get(i).getId()==Integer.parseInt(id))
				return this.subtasks.get(i);
		}
		return null;
	}
	

}
