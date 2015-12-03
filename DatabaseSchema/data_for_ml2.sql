CREATE DEFINER=`root`@`localhost` PROCEDURE `data_for_ML2`(
	exp_ID			BigInt,
    Lim				Int,
    ModBy			int,
    clockBiggerThan	int
)
BEGIN

	DECLARE done INT DEFAULT FALSE;
  
	DECLARE bkd_ID	INT;
  
	DECLARE cur1 CURSOR FOR 
		Select	b.ID			AS	backend_ID#,
				#ex.ID			As	experiment_ID,
				#ex.comment		As	experiment_description,
				#b.description	As	backend_description
			From	backend		b
						Inner	Join
					experiment	ex
						On	b.experiment_id	= ex.id		And
							ex.id			= exp_ID;
                        
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	DROP TEMPORARY TABLE IF EXISTS pinged_volumes;

	CREATE	TEMPORARY	TABLE	pinged_volumes
		(
			clock				BigInt	NOT NULL,
			Backend_ID			BigInt	NOT NULL,
			VolumesCount		Int	Not	Null,#Int	Not	Null,
			SLAViolationCount	Int	Not	Null,#Int	Not	Null,
			TotalAvailableIOPS	Int	Not	Null,#Int	Not	Null,
			TotalRequestedIOPS	Int	Not	Null#Int	Not	Null,
			#,index (Clock, Backend_ID)
		);
        
	Set	@VC = null;
	Set @SVC = null;
	Set @TAI = null;
	Set @TRI = null;
	Set @clock = null;
	Set @rnk = null;
	Set @rnk2 = null;

	Insert	Into	pinged_volumes

		Select	Clock,
				Backend_ID,
                VolumesCount,
                SLAViolationCount,
                TotalAvailableIOPS,
                TotalRequestedIOPS
                
				From	(
						Select	Clock																			As 	Clock,
								Backend_ID																		As	Backend_ID,
								@VC:= if(@clock = Clock, @VC + VolumesCount, VolumesCount)						As	VolumesCount,
								@SVC := if(@clock = Clock, @SVC + SLAViolationCount, SLAViolationCount)			As	SLAViolationCount,
								@TAI := if(@clock = Clock, @TAI + TotalAvailableIOPS, TotalAvailableIOPS)		As	TotalAvailableIOPS,
								@TRI := if(@clock = Clock, @TRI + TotalRequestedIOPS, TotalRequestedIOPS)		As	TotalRequestedIOPS,
								@clock:=Clock
								
							From	(
										Select	clock									As	clock,
												Backend_ID								As	Backend_ID,
												count(*)								As	VolumesCount,
												Sum(Is_SLA_Violation)					As	SLAViolationCount,
												sum(Available_IOPS)						As	TotalAvailableIOPS,
												sum(Volume_Requested_IOPS)				As	TotalRequestedIOPS
											From	(
														Select	VPM.clock																As	clock,
																VPM.Backend_ID															As	Backend_ID,
																VPM.SLA_violation														As	Is_SLA_Violation,
																VPM.available_IOPS														As	Available_IOPS,
																v.IOPS																	As	Volume_Requested_IOPS,
																@rnk := if(@clock = VPM.clock, if(@rnk2 % 4 = 0, @rnk+1, @rnk+0), 1)	As 	Rank,
																@rnk2 := if(@clock = VPM.clock, @rnk2 + 1, 1)							As 	Rank2,
																@clock := VPM.clock
																#,count(VPM.clock)					As	VolumesCount
																#,Sum(VPM.SLA_violation)		As	SLAViolationCount,
																#sum(VPM.available_IOPS)		As	TotalAvailableIOPS,
																#sum(v.IOPS)					As	TotalRequestedIOPS
																
															From	volume_performance_meter		VPM
																		Inner	Join
																	volume							v
																		On	VPM.volume_ID	= v.ID
															Where	VPM.experiment_id	= exp_ID# and VPM.Backend_ID	= 427964
																	#And
                                                                    #VPM.clock			> clockBiggerThan
															
															Order	By	clock				Asc,
																		VPM.Backend_ID		Asc
													)	As inn1
                                                    
											Group	By	clock,
														Backend_ID,
                                                        Rank
									)	As inn2
						)	As	inn3
			;

	OPEN cur1;
    
	read_loop: LOOP
    
		FETCH cur1 INTO bkd_ID;
    
		IF done THEN
			LEAVE read_loop;
		END IF;
    
		Select	bkd_ID;
       
		set	@rnkOrder  = 0;
       
		Select	*
			From	(
            
						Select	*
							From	(
										Select	Case
													When	ModBy	= 0	Then	PV.clock
													Else						Mod(PV.clock, ModBy)	
												End									As	Clock,
												PV.VolumesCount,
												PV.SLAViolationCount,
												PV.TotalRequestedIOPS,
												PV.Backend_ID,
												PV.TotalAvailableIOPS,
												PV.clock							As	Clock_No_Mod,
												@rnkOrder := @rnkOrder + 1			As	RankOrder
												
											From	pinged_volumes		PV
											Where	PV.Backend_ID	= bkd_ID
													
											Order	By	PV.Clock	Asc
									)	As	Inn
								Order	By	RankOrder	Desc
								Limit	clockBiggerThan
								#Where	Inn.RankOrder	> clockBiggerThan;
					)	As	Inn2
			Order	by	RankOrder	Asc;
    
	END LOOP;

	CLOSE cur1;
END