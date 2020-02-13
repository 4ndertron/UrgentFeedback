WITH HOMEBUILDER_ACCOUNTS AS (
    SELECT P.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FIRST_NAME                                                                      AS CUSTOMER_1_First
         , ''                                                                                 AS Customer_1_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie')) AS CUSTOMER_1_SUFFIX
         , TRIM(REPLACE(CT.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))                            AS CUSTOMER_1_LAST
         ----------------------------------------------------------
         , CNT.FIRST_NAME                                                                     AS CUSTOMER_2_First
         , ''                                                                                 AS Customer_2_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CNT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1,
                                 'ie'))                                                       AS CUSTOMER_2_SUFFIX_NAME
         , TRIM(REPLACE(CNT.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, '')))                      AS CUSTOMER_2_LAST_NAME
         ----------------------------------------------------------
         , P.SERVICE_ADDRESS
         , P.SERVICE_CITY
         , P.SERVICE_COUNTY
         , P.SERVICE_STATE
         , P.SERVICE_ZIP_CODE
         , TO_DATE(P.INSTALLATION_COMPLETE)                                                   AS INSTALL_DATE
         , TO_DATE(CON.TRANSACTION_DATE)                                                      AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(INSTALL_DATE))                                          AS TERMINATION_DATE
         , CON.RECORD_TYPE                                                                    AS CONTRACT_TYPE
         , ROUND(DATEDIFF('D', '2013-11-02', TO_DATE(P.ESCROW)) / 7, 0)                       AS WEEK_BATCH
         , 'New Account'                                                                      AS BATCH_TYPE
    FROM RPT.T_PROJECT AS P
             LEFT JOIN
         RPT.T_CONTRACT AS CON
         ON P.PRIMARY_CONTRACT_ID = CON.CONTRACT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON P.CONTRACT_SIGNER = CT.CONTACT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CNT
         ON P.CONTRACT_COSIGNER = CNT.CONTACT_ID
    WHERE P.SERVICE_NUMBER IN
          ('4730982', '4095812', '2412599', '998036S', '2882438', '2823244', '2134012', '2834011S', '2138824',
           '2838433', '3335523', '1900753', '2904109', '3196638', '2937837', '3004505', '2983391S', '2470372S',
           '3458162S', '3413409S', '4275221', '1919824S', '2125090', '2956776', '3375916S', '3268312', '3392938',
           '3452505', '3450112', '3374071S', '3427794', '3443343', '3469175', '3462708', '5990149', '3497032',
           '3458543', '3540606', '3481317', '2900481', '3480064S', '3466015', '3573770', 'S-2907946S', '2319043S',
           '3595220S', '3537565', '3495614S', '3706582', '3635990', '3176864S', '3428764', '3745806', '3228366',
           '3596182S', '3447911S', '3602439', '3699606', '3609843S', '3618371S', '3613430', '3604021', '5814847',
           '1386760', '3637384', '3252222', '2115549', '3710754', '3601994S', '3707981', '3714855', '3061507',
           '3651700', '3628238', '3819090', '2246614', '3785323', '3718503', '3689483', '3770600', '3739340', '3654094',
           '3752776', '3859780', '3782563', '3885459', '3713873', '3602458', '3236323', '3858903', '3813311', '3164375',
           '3469785', '3406170', '3406170S', '3758886', '3745788', '3635450S', '3834265', '3719418', '3562546',
           '3962264', '3531774', '3656914', '3859271', '3785989', '3858760', '3348934', '3348934S', '3978505',
           '3837074', '3720996', '3798225', '3776332', '3655061', '3987068', '3772478', '3938868', '3987719', '3714649',
           '3656462', '3902449', '3752503', '3893226', '3902169', '3808826', '3576924', '4006337', '3981835', '3631475',
           '3898158', '3783111', '3990295', '3918660', '3672078', '3782107', '3463084S', '3670036', '4012000',
           '3624860s', '3791458', '4064559', '3901689', '3604625', '3961660', '3744384', '3991783', '4015404',
           '3907771', '4057184', '4043793', '4039413', '3694814', '4038978', '4067844', '4052659', '3937732', '3885708',
           '4017566', '4017039', '3798034', '4071321', '4038414', '4080757', '3777260', '4093909', '3578439S',
           '4041991', 'S-5868570', '5868570', '4061383', '4020058', '4044919', '4103984', '3653376', '4039121',
           '4105191', '3843913', '4091030', '4045117', '4118095', '865596', '2373564', '3469787S', '4032083', '4015063',
           '3685697', '4122047', '4077030', '4007798', '2934747S', '3002878', '2945796', '2796381', '3677268',
           '4017444', '5265815', '4080358', '4015348', '4039188', '4141592', '2899851', '4468076', '3597021', '3878322',
           '4072107', '4069373', '3987452', '3908045', '4136036', '4082730', '4043983', '4057217', '3887482', '4006386',
           '4116556', '4184059', '4072554', '3698212', '4188840', '4114918', '4110960', '4077735', '4235362', '4101855',
           '4229048', '3464070', '4127190', '4080315', '4222239', '4248691', '4210584', '4225772', '4183953', '3942194',
           '3795114', '3901754', '4235261', '4231064', '4106682', '4843778', '4137127', '4260293', '4254427', '3906955',
           '4194576', '4111718', '3729300', '4316781', '4274780', '3655066', '4066835', '4126884', '4273616', '4111751',
           '4231614', '5953927', '4060733', '4280091', '4192481', '4087225', '4433496', '3962406', '4312768', '4110336',
           '4241005', '4235097', '4405885', '4325288', '4203642', '4019255', '3640859S', '4270752', '4417282',
           '4419584', '4482085', '5812006', '4015869', '4419059', '4272964', '4222604', '4303650', '4413406', '4274741',
           '4270531', '4275964', '4438629', '4459688', '4193667', '4459858', '6005304', '4255127', '4364852', '4251829',
           '4317797', '4317713', '4502397', '4368270', '4221842', '4394450', '4498686', '4289010', '4374549', '4314269',
           '4459405', '4082445', '4249780', '4337442', '4373699', '4425465', '4147640', '4576983', '4516996', '4216454',
           '4516009', '4198573', '4434391', '4409576', '4290036', '4509031', '4476932', '4497201', '4276413', '4202444',
           '4202465', '4491228', '4296626', '4555945', '4484346', '4444121', '4620335', '4611456', '4572147', '4428206',
           '4202325', '4583741', '5121524', '2777977S', '3739354', '4277602', '4607741', '4508411', '4468453',
           '4118086', '4308748', '4012086', '4510332', '4528818', '4131102', '4613845', '3947876', '4540682', '4331521',
           '4530498', '4459816', '4115781', '3741079', '4572660', '4558519', '4612175', '4567900', '4147858', '4645867',
           '4658458', '4666355', '4639719', '4642166', '4619188', '4685565', '4664425', '4140909', '4046456', '4620055',
           '4531839', '4093304', '4656494', '4568508', '4682370', '3565306S', '3871166', '2893975', '6032263',
           '4384023', '4649616', '4646032', '4535782', '4611699', '4123046', '4676180', '4677785', '4654092', '4574174',
           '4740020', '3917348', '4630486', '4681691', '4301004', '4137624', '2578732S', '4641955', '4598926',
           '4617785', '4499928', '4054386', '4707153', '4668156', '1016033', '4115485', '4139695', '4591142', '4443788',
           '4680357', '4662703', '4724242', '4159005', '3261103S', '4609033', '4657064', '3479522', '4608963',
           '4715908', '4604186', '4485773', '4686409', '4682221', '3347426', '4088599', '4473517', '4738589', '4332739',
           '3607420', '1835018S', '3527163', '4068092', '4635150', '4684968', '4670839', '4742411', '4693651',
           '4660468', '4778838', '3672179', '4757945', '4341911', '3352328', '4665345', '4476703', '4737912', '4185405',
           '4774123', '4725001', '4711379', '4768016', '4779200', '4729346', '4755618', '4749629', '4798746', '4726328',
           '4767184', '4811950', '4643024', '4282240', '4806313', '4783155', '4793920', '4662068', '4782207', '4758023',
           '4780235', '4811880', '4577173', '6015976', '5927874', '4792202', '4724026', '4769638', '4753354', '4753337',
           '4781418', '4687074', '4766417', '4689437', '4682700', '4810651', '4696163', '4680722', '4668416', '4628748',
           '4472309', '4799645', '4800107', '4790582', '4817736', '4805313', '4725984', '4836954', '4859771', '4676309',
           '4617595', '4848206', '4751179', '4862391', '4833554', '4753509', '4806412', '4835399', '4749310', '4809457',
           '4431414', '4810756', '4603613', '4695903', '4716887', '4808534', '4834951', '4775863', '4724917', '4845977',
           '4790587', '4675507', '4684486', '4867966', '4850246', '4873965', '4989242', '4708803', '4809446', '4883711',
           '4754335', '4221865', '4873123', '4505455', '4716150', '590059', '4893929', '4942585', '4818065', '4854637',
           '4850828', '4898550', '4897515', '4879993', '4853106', '4934758', '4836904', '4888608', '4845771', '4864118',
           '3110830S', '4878039', '4869549', '4975595', '4836818', '4854573', '4912568', '4782245', '4879177',
           '4891920', '4813797', '4938167', '4818719', '4862741', '4934642', '4974280', '4932750', '4926645', '4941879',
           '4983189', '4849312', '4703999', '4847831', '4904835', '4064823', '4717160', '4892938', '5004447', '4781895',
           '5036490', '4877843', '4871417', '4981368', 'S-4883997', '4951349', '4940609', '4892672', '6206766',
           '4918294', '4970023', '5054186', '5035957', '4974496', '5097478', '4993545', '4831973', '5081353', '4854622',
           '4674142', '4885127', '4845555', '5005560', '4985648', '4921492', '5089499', '4859855', '4812429', '4909757',
           '4511058', '5021301', '5093531', '4996193', '4925927', '4858560', '4997028', '4960871', '4892333', '5017061',
           '4999470', '5111611', '5111563', '5144718', '5986708', '4978587', '5118627', '5146503', '4586794', '4964596',
           '4635302', '4876156', '4924354', '5011856', '5147552', '4879046', '4999103', '4677830', '5143900', '3969833',
           '4288782', '5092886', '5158609', '5267382', '4443688', '5283353', '4815412', '5270649', '4906760', '5227384',
           '3170099', '5115959', '5070108', '4884193', '5163095', '5218809', '4056681', '5261143', '5057053',
           '2855161S', '5254786', '3428772', '4637193', '3391000S', '5149645', '4817922', '5271009', '5290066',
           '3748180', '3590373S', '5091110', '5321018', '5287290', '4115930', '5009236', '5280843', '4791454',
           '5132022', '5501886', '5312565', '5218696', '5225676', '4793083', '5222070', '5138011', '5254414', '5275927',
           '5277123', '4855708', '4827005', '5105756', '5191880', '5274478', '5194711', '5255061', '5312406', '5225307',
           '5090786', '5327215', '5293709', '5320921', '5340589', '5315429', '5303656', '3473509', '5322756', '4135568',
           '4127042', '5200167', '5355719', '4816371', '3731925', '3751681', '5287029', '5369330', '5338119', '5266678',
           '5301824', '5409020', '4694423', '5312139', '5358772', '5296038', '5373713', '4874840', '5342456', '4194264',
           '5329320', '5403942', '2722964', '5405561', '4903291', '3617817', '4913294', '5056764', '5374574', 'Colonie',
           '4565286', '5318127', 'S-5296980', '5296980', '5353258', '5307266', '5062494', '3952371', '5415286',
           '4088956', '5382269', '5404893', '5347091', '5401432', '5427165', '4705990', '5391209', '5969816', '3905913',
           '5240617', '5286759', '5453894', '4751355', '4468064', '5407722', '5384630', '5043395', '5419429', '4068265',
           '5412191', '5403430', '5302066', '5390593', '5390173', '4123064', '5427892', '5430235', '4540057', '5219262',
           '5377076', '5411229', '5386936', '5431181', '5438423', '1924730S', '4638104', '4724653', '5449007',
           '4241334', '5466166', '5506170', '5419350', '5323597', '5439810', '5421789', '4455604', '5446601', '5429001',
           '5886132', '5505082', '5422122', '4109365', '5327002', '5526853', '4534445', '5479305', '5472449', '5459980',
           '4969330', '5470946', '5430576', '3733685', '5505973', '5271131', '5489559', '5489566', '4060515', '5441935',
           '5503181', '5514007', '4418919', '4505563', '5460576', '5503751', '5532408', '4202070', '4927352', '5546051',
           '2692049', '5508652', '5455198', '5504647', '2709038', '5488337', '5550900', '4267093', '5526786', '5494736',
           '5436931', '5526276', '5527223', '5548406', '4035154', '5488027', '4812778', '4763200', '5436268', '5551167',
           '4416616', '4590980', '2644058S', '4652403', '5458897', '5220539', '4982977', '5561697', '3763159',
           '2644058', '5555090', '5472151', '5437321', '5527387', '4011599', '5594694', '5421977', '5698908', '5597425',
           '5564223', '5614841', '5458924', '4736071', '5535985', '5575200', '4706721', '5462863', '5694329', '5656386',
           '4176783', '5424309', '5526583', '5697039', '5499904', '5733197', '5548471', '5711094', '5493163', '5449491',
           '4835951', '5438629', '5760657', '5720301', '5724227', '5722778', '5802232', '5083248', '5718355', '5803160',
           '5510618', '5754152', '5802134', '5751896', '5747923', '3709883', '5754577', '5517713', '5464642', '5804004',
           '2605051', '5800702', '3600061', '5660481', '4905973', '5709489', '5207575', '5794685', '5808758', '5797857',
           '5799243', '4684506', '5635540', '2797857', '5725681', '5801740', '3973843', '5815315', '5799339', '5802905',
           '4019215', '5806785', '4693798', '5812979', '5167632', '5710635', '5801737', '5689914', '4374095', '5489821',
           '5812492', '5805797', '5522791', '3292471', '5819198', '4668428', '4098125', '5823926', '3188403S',
           '5814832', '5818136', '5815917', '3891534', '5814765', '5837181', '5814911', '5704536', '7022719', '5817241',
           '5808221', '5637894', '5927142', '5828442', '5798194', '4110637', '4862843', '5360917', '2646018S',
           '3884602', '5806623', '5823445', '5460675', '5678382', '4690724', '4067652', '4100541', '5531347',
           '2497842S', '3537960', '5832110', '5820352', '5826677', '3628585', '4640355', '5017369', '5831414',
           '5843642', '5828315', '2509744S', '5831449', '5845010', '5498195', '5828954', '5667884', '5822733',
           '5832103', '4551973', '5839627', '4020777', '4608809', '5694570', '4201405', '5842880', '5839429', '5840319',
           '3458216', '4490103', '4038975', '5830018', '5565405', '5804483', '5833615', '5859193', '5856189', '3695756',
           '3869644', '5833108', '5856200', '5856202', '4938547', '5856176', '4602159', '3744152', '4618549', '4899005',
           '5852541', '5854765', '2834011', '5865432', '5519503', '5858223', '5793466', '5856662', '5857678', '5842931',
           '5857188', '5861185', '5868196', '5856425', '5857684', '5858678', '5857892', '5856878', '5807932', '5859230',
           '3929988', '5845035', '5852317', '5868245', '5864003', '5703900', '5868478', '5865237', '5862887', '5865056',
           '5870873', '4080692', '5869308', '5867616', '5851967', '5868061', '6001334', '5859993', '5849384', '5240754',
           '3618185S', '3549029S', '5871664', '3809464', '5928854', '5871471', '5813711', '3342031', '5647361',
           '5874289', '5877311', '5865207', '4925644', '5530755', '5877793', '5867135', '5806074', '3618185', '5875806',
           '4044370', '5876069', '5837867', '5299286', '5885787', '2878851', '5863712', '5868711', '5883254', '5276683',
           '2914903', '3679554', '5877071', '5875419', '6049555', '5875682', '5853421', '5876817', '5864173', '5889992',
           '5041083', '5868841', '5881803', '4593659', '3210537S', '5887977', '5883661', '5864014', '4394036',
           '5883808', '5882553', '5876790', '5883859', '4324350', '5869992', '5880742', '5885851', '5883475', '5881414',
           '5890761', '5893722', '5886154', '5883856', '5887324', '5899709', '5869104', '5900059', '4910775', '5807002',
           '5902841', '5869336', '5888274', '5890342', '5849915', '5892816', '5860446', '5889528', '4681064', '5893279',
           '5850628', '5885849', '5889542', '5889776', '5893581', '5883540', '5693747', '5890163', '5881764', '5897364',
           '5901211', '5902264', '5902142', '5336262', '5895211', '5881695', '5893275', '5899403', '5915647', '5912522',
           '5886384', '5897214', '5887475', '6082500', '4648007', '5918805', '4271577', '5888934', '5916578', '5866553',
           '4736761', '4626473', '2903086', '5904321', '5896469', '5906938', '5798105', '6074710', '5907615', '4172039',
           '5910859', '4063074', '5907460', '4645896', '4060069', '5905627', '5909370', '5909381', '5905631', '4795028',
           '5912468', '5909338', '5909343', '5889348', '5911192', '5921840', '4240655', '4679248', '4305899', '5898329',
           '5917511', '5922241', '5905066', '5912605', '5902384', '5904953', '5931132', '5915085', '5922917', '5916613',
           '5928449', '5933118', '5897537', '5925340', '3773417', '5931323', '5923177', '5910574', '5896115', '5904734',
           '5906007', '5910092', '4850474', '5917607', '5927567', '5929016', '5912213', '5861581', '5920036', '5919907',
           '5919886', '5935512', '3886591', '5931120', '5258222', '5941137', '5914138', '4707817', '5912115', '5812938',
           '5941259', '5897818', '5952449', '5939638', '5840369', '5922781', '586916', '5934624', '5905336', '5918818',
           '5240117', '5933136', '5932027', '5908958', '5884498', '4841799', '5914240', '4808607', '271003', '4545264',
           '4221914', '5957485', '5920159', '2815750', '5925791', '5956099', '5916360', '5934724', '5899606', '5926475',
           '5926321', '4644040', '5945646', '5898198', '5935952', '5911164', '4934706', '5919919', '5943112', '5927980',
           '5927945', '5919345', '5924333', '5936479', '5903806', '5943891', '5924549', '5954692', '5957281', '5958935',
           '5942738', '5938540', '5894847', '5936155', '5938411', '5924359', '4790793', '5958790', '5928424', '5953487',
           '5928363', '5960316', '5968838', '5940390', '5933883', '5937866', '5965419', '5943421', '5943462', '5974725',
           '5987536', '5948547', '5973503', '5938337', '5955687', '5909145', '5952782', '5967964', '5940281', '5941630',
           '5953164', '5369977', '5943384', '5913033', '5451141', '5956956', '5937981', '5922019', '5946001', '5980676',
           '5953095', '5929978', '3513412S', '5979958', '5973671', '5966382', '5933073', '3606944', '5977891',
           '5955210', '5837273', '5952550', '4621417', '5956787', '5972526', '5931338', '4604557', '4883973', '5979057',
           '5989210', '5885294', '5962444', '5955806', '5960813', '5972306', '5413385', '5955801', '5807112', '5895902',
           '5955890', '5977806', '5957656', '5942734', '5963577', '5338580', '5955877', '5953769', '5981570', '5949009',
           '6000352', '5956870', '5969281', '5968815', '5962268', '6003133', '5961726', '5989422', '4656526', '5962729',
           '5957324', '5984772', '5965195', '5991940', '5908502', '5959080', '5935058', '5956289', '5928217', '6005534',
           '5976363', '6009322', '5885562', '5984904', '5979347', '5566632', '5450766', '6004474', '5966432', '5969938',
           '5999724', '5824510', '5989952', '5804166', '5966420', '5981619', '5968327', '3248436S', '5857364',
           '5965231', '5969801', '5965271', '6002241', '5989960', '5972146', '5998972', '5983724', '5972292', '5976352',
           '5999141', '5966615', '5981697', '6016115', '5977882', '5983558', '5273109', '5969155', '4990485', '6004940',
           '4243546', '5964365', '6003537', '5998953', '2890388', '5968337', '3571455', '5939209', '6012004', '5986857',
           '5989211', '5972161', '4244281', '6013314', '5919524', '5930420', '6023858', '6010620', '4834188', '4589856',
           '6014851', '6000505', '6000931', '5934818', '5986895', '6010565', '5218015', '6002801', '5966349', '5989631',
           '4015374', '5989525', '3598071', '5983009', '5984619', '5541229', '5990155', '5986662', '5985870', '5990929',
           '5989513', '5986661', '6008975', '5865270', '4725636', '5987788', '3253777', '5985987', '5986719', '5839375',
           '5986678', '5861787', '5985855', '6021807', '5960878', '3567793', '5800996', '6031807', '6020405', '4875753',
           '6031291', '6004626', '5991072', '6012606', '5985915', '5985893', '6020783', '6007515', '5875485', '5991813',
           '5998015', '6087151', '6011219', '6002647', '6012379', '6006599', '5999294', '6009752', '5991751', '4396446',
           '4359263', '4844574', '6064836', '5021008', '6011705', '5365555', '6037120', '4096443', '6014148', '6012061',
           '6035767', '6015208', '6020574', '6013668', '5792397', '5976514', '6038545', '5988350', '5504549', '4114143',
           '6026736', '6039056', '5886004', '6038487', '6014113', '6034765', '5964622', '5983288', '6027837', '6022136',
           '6043474', '6030148', '6030394', '6049760', '6044033', '6051687', '5934314', '6053914', '6030693', '6064566',
           '6063696', '6003043', '6059206', '6053765', '6052316', '6054653', '6051270', '6056774', '6023717', '6085502',
           '4272522', '6072111', '6024513', '6043101', '6056147', '6076294', '6014840', '6066896', '6065334', '6057418',
           '6066243', '6033999', '5884594', '6065736', '6069995', '6054488', '6075117', '6059469', '6034248', '6041787',
           '6066530', '6078779', '4902166', '5292611', '6015526', '6070361', '6070266', '5948852', '6059798', '6040602',
           '6084557', '6060676', '6070277', '6081516', '6088062', '4324426', '6085350', '6082620', '5941209', '6083958',
           '6087403', '4919446', '4915569', '5986722', '6092081', '5904637', '4878498', '6008023', '5884395', '6085925',
           '6055181', '6061704', '6081204', '6116204', '6106211', '6089745', '6093054', '6091411', '6066791', '6103840',
           '6133791', '6078648', '6036002', '6147565', '6110058', '6101110', '6091159', '6109983', '6034924', '6024816',
           '6109605', '6142977', '6083596', '6115217', '6119207', '6101076', '5887247', '6021440', '6035934', '6123722',
           '6103083', '6089800', '6091201', '5998705', '6043579', '6016778', '6033217', '6047196', '6019723', '6043012',
           '6027826', '6118726', '6038784', '6018569', '6095555', '6113976', '6083206', '6090221', '6064094', '6117721',
           '6019704', '6118538', '6039377', '6100485', '6026845', '6095353', '6020078', '6078226', '6026187', '6077568',
           '6018576', '6021623', '6113211', '6034125', '6109566', '6037536', '6030140', '6019641', '6144508', '6121441',
           '5929568', '6132161', '6106752', '6133172', '6043211', '6139603', '6125366', '6155314', '6104452', '6116176',
           '6138918', '6138025', '6148823', '6169910', '6142308', '6172360', '5960925', '3620556S', '4845859',
           '3709653', '6062740', '3249912', '5859204', '4914096', '5929005', '3635760', '4507570', '3893412', '5942065',
           '3292469', '5927335', '5231092', '5905827', '5006877', '5965278', '5996282', '6046898', '6079699', '6114282')
)

   , MERGE AS (
    SELECT *
    FROM (
                 (SELECT * FROM HOMEBUILDER_ACCOUNTS)
         )
)

   , MAIN AS (
    SELECT *
    FROM MERGE
)

SELECT *
FROM MAIN