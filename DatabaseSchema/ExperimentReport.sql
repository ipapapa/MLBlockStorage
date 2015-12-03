CREATE DEFINER=`root`@`localhost` PROCEDURE `experiment_report`(
	exp_ID		bigint
)
BEGIN
	Select	report.AllRequests,
			report.ScheduledVolumes,
			report.AllRequests - report.ScheduledVolumes	As	RejectedVolumes,
			report.DeletedNumber,
			report.AVGCapacity,
			report.DeleteAverageTime,
			report.SLA_Violation_Count,
			report.Count_Volume_Performance_meter,
			report.Max_Clock,
			report.Backend_Count,
            report.IOPSRequesteAverage,
            report.AvailableIOPSAverage
		From
			(
				Select	
					(
					Select	Count(*)				AS	ScheduledVolumes
						From	schedule_response		SR
									Inner	Join
								Volume					V
									On	SR.Experiment_ID			= exp_ID	And
										v.schedule_response_ID		= SR.ID
					)		AS	ScheduledVolumes,
					(
					Select	Count(*)				As	AllRequests
						From	schedule_response		SR
									Where	SR.Experiment_ID			= exp_ID
					)		As	AllRequests,
					(
					Select	Avg(VR.capacity)				As	AllRequests
						From	schedule_response		SR
									Inner	Join
								volume_request			VR
									On	SR.volume_request_ID	= VR.ID
						Where	SR.Experiment_ID			= exp_ID
					)		As	AVGCapacity,
					(
					Select	Count(*)				AS	ScheduledVolumes
						From	schedule_response		SR
									Inner	Join
								Volume					V
									On	SR.Experiment_ID			= exp_ID	And
										v.schedule_response_ID		= SR.ID			And
										v.is_deleted				= 1
					)		AS	DeletedNumber,
					(
						Select	Avg(V.delete_clock - SR.clock)		As	DeleteAverageTime
							From	schedule_response		SR
										Inner	Join
									Volume					V
										On	V.is_deleted				= 1				And
											SR.Experiment_ID			= exp_ID	And
											v.schedule_response_ID		= SR.ID
					)		As	DeleteAverageTime,
					(
						Select	count(*)
							From	volume_performance_meter	VPM
							Where	VPM.experiment_id		= exp_ID	And
									VPM.SLA_violation		= 1
					)		As	SLA_Violation_Count,
					(
						Select	count(*)
							From	volume_performance_meter	VPM
							Where	VPM.experiment_id		= exp_ID
					)		As	Count_Volume_Performance_meter,
					(
						Select	max(VPM.clock)
							From	volume_performance_meter	VPM
							Where	VPM.experiment_id		= exp_ID
					)		As	Max_Clock,
					(
						Select	Count(*)		As	Backend_Count
								
							From	backend		b
										Inner	Join
									experiment	ex
										On	b.experiment_id	= ex.id		And
											ex.id			= exp_ID
					)		As	Backend_Count,
                    (
						Select	Avg(V.IOPS)		As	IOPSRequesteAverage
							From	schedule_response		SR
										Inner	Join
									Volume					V
										On	V.is_deleted				= 1				And
											SR.Experiment_ID			= exp_ID	And
											v.schedule_response_ID		= SR.ID
					)		As	IOPSRequesteAverage,
                    (
						Select	avg(VPM.available_IOPS)		As	AvailableIOPSAverage
							From	volume_performance_meter	VPM
							Where	VPM.experiment_id		= exp_ID
					)		As	AvailableIOPSAverage
			)	As report;
END