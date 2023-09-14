//package com.esi.DroolsDataQuality.pipeline.dagengine;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.Serializable;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Spliterator;
//import java.util.Spliterators;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import java.util.stream.StreamSupport;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.functions;
//import org.apache.spark.sql.types.StructType;
//import org.kie.api.KieBase;
//import org.kie.api.KieServices;
//import org.kie.api.definition.KiePackage;
//import org.kie.api.definition.rule.Rule;
//import org.kie.api.runtime.KieContainer;
//import org.kie.api.runtime.KieSession;
//import org.kie.api.runtime.rule.FactHandle;
//
//import com.esi.DroolsDataQuality.pipeline.configuration.FileSourceConfig;
//
//import lombok.extern.slf4j.Slf4j;
//
//
///**
// * Created by EH8220 on 10/1/2019.
// */
//
//@Slf4j
//public class DroolsPlugin implements Serializable {
//	/**
//	 *
//	 */
//	private static final long serialVersionUID = -6754046641461673745L;
//	int i = 0;
//	Object objInstance = null;
//	int index = 0;
//	Object nameObjInstance = null;
//
//	public KieBase loadRules() throws Exception {
//
//		KieContainer kieContainer = null;
//
//		try {
//			KieServices kieServices = KieServices.Factory.get();
//			kieContainer = kieServices.getKieClasspathContainer();
//
//		} catch (Exception e) {
//			throw e;
//		}
//		return kieContainer.getKieBase();
//	}
//
//	public void getRuleNames(KieBase kieBase) throws Exception {
//		try {
//			log.info("Created kieBase..");
//			for (KiePackage kp : kieBase.getKiePackages()) {
//				for (Rule rule : kp.getRules()) {
//					log.info("KiePackage: " + kp + " rule: " + rule.getName());
//				}
//			}
//		} catch (Exception e) {
//			throw e;
//		}
//	}
//
//	public void setAgendaGroupFocus(KieSession ksession, FileSourceConfig conf) throws Exception {
//
//		try {
//			String AgendaGroups = conf.getProperty("drools.agenda.groups");
//			List<String> groupsList = Arrays.asList(AgendaGroups.split("\\s*,\\s*"));
//
//			if (groupsList.size() > 0) {
//				for (int i = 0; i < groupsList.size(); i++) {
//					String ag = groupsList.get(i);
//					ksession.getAgenda().getAgendaGroup(ag).setFocus();
//					ksession.fireAllRules();
//				}
//			} else {
//				log.info("No Focus Groups Found. Firing All Rules...");
//				ksession.fireAllRules();
//			}
//		} catch (Exception e) {
//			throw e;
//		}
//	}
//
//	public Object getModelInstance(Row row, ArrayList<String> columnList, List<Method> methodList, Object objInstance) throws ReflectiveOperationException {
//		try {
//			//LocalDateTime start2 = LocalDateTime.now();
//			for (i = 0; i < columnList.size(); i++) {
//				for (Method M : methodList) {
//					if (M.getName().equalsIgnoreCase(columnList.get(i))) {
//						Object oj = null;
//						if (row.getAs(i) != null) {
//							oj = row.getAs(i);
//							M.invoke(objInstance, oj);
//						}
//						break;
//					}
//				}
//			}
//			/*		LocalDateTime end2 = LocalDateTime.now();
//			System.out.println("Total Time Taken For getModelInstance in Millis ::"+ChronoUnit.MILLIS.between(start2, end2));
//			 */
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			throw ex;
//		}
//		return objInstance;
//	}
//
//	public Object[] callAllGetterMethods(Object row, ArrayList<String> columnList, List<Method> methodList) throws Exception {
//		Object[] getvalues = new Object[methodList.size()];  //if the model which you are using for setting and getting value extends super class then add .getSuperclass()
//		index = 0;
//		try {
//			//LocalDateTime start1 = LocalDateTime.now();
//			columnList.stream().forEach(S -> {
//				methodList.forEach(M -> {
//					if (M.getName().equalsIgnoreCase("get" + S) || M.getName().equalsIgnoreCase("is" + S)) {
//						Object str;
//						try {
//							str = M.invoke(row);
//							getvalues[index] = str;
//							index = index + 1;
//							return;
//						} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
//							e.printStackTrace();
//						}
//					}
//
//				});
//			});
//
//			/*LocalDateTime end1 = LocalDateTime.now();
//			System.out.println("Total Time Taken For Stream callAllGetterMethods in Millis ::"+ChronoUnit.MILLIS.between(start1, end1));
//			 */
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			throw ex;
//		}
//		return getvalues;
//	}
//
//	public Dataset<Row> executeDrools(Dataset<Row> Droolsservicesdf, SparkSession spark, FileSourceConfig fsConfig, String droolsSession, String droolsSchema, String droolsSetColumns) throws Exception {
//		Dataset<Row> DroolsDataset = null;
//		try {
//			Dataset<Row> df = Droolsservicesdf.drop("LAST_UPDATE_DT");
//
//			JavaRDD<Row> DroolsRowRDD = null;
//			JavaRDD<Row> bronzeTableRDD = df.javaRDD();
//
//			ArrayList<String> getColumnList = new ArrayList<>(Arrays.asList(df.columns()));
//			ArrayList<String> setColumnList = new ArrayList<>(Arrays.asList(df.columns()));
//
//			//Adding derived columns to setColumnList
//			//	String columns = fsConfig.getDroolsColumnList();
//			String[] items = droolsSetColumns.split("\\s*,\\s*");
//			setColumnList.addAll(Arrays.asList(items));
//
//			DroolsRowRDD = bronzeTableRDD.mapPartitions((partitions) -> {
//
//				KieServices kieServices = KieServices.Factory.get();
//                KieContainer kContainer = kieServices.getKieClasspathContainer();
//                KieSession ksession = kContainer.newKieSession(fsConfig.getProperty("drools.sessionname"));
//
//				/*KieServices kieServices = KieServices.Factory.get();
//				KieContainer kContainer = kieServices.newKieContainer(kieServices.newReleaseId(fsConfig.getProperty("drools.groupId"), fsConfig.getProperty("drools.artifactId"), fsConfig.getProperty("drools.version")));
//				KieSession ksession = kContainer.newKieSession(droolsSession);*/
//
//				List<Row> resRow = new ArrayList<>();
//				String modelName = (fsConfig.getProperty("drools.modelname"));
//
//				objInstance = Class.forName(modelName).newInstance();
//				Method[] method = objInstance.getClass().getSuperclass().getDeclaredMethods();
//
//				List<Method> setMethodList = Arrays.stream(method).filter(S -> S.getName() != null && S.getName().contains("set")).collect(Collectors.toList());
//				List<Method> getMethodList = Arrays.stream(method).filter(S -> S.getName() != null && (S.getName().contains("get") || S.getName().contains("is"))).collect(Collectors.toList());
//				ArrayList<String> newGetColumnList = (ArrayList<String>) getColumnList.stream().map(S -> S = "set" + S).collect(Collectors.toList());
//
//				List<String> tempGetColumnList = new ArrayList<String>();
//				tempGetColumnList.addAll(newGetColumnList);
//
//				List<Method> temp = new ArrayList<Method>();
//				temp.addAll(setMethodList);
//
//				setMethodList.stream().forEach(m -> {
//					tempGetColumnList.stream().forEach(l -> {
//						if (l.equalsIgnoreCase(m.getName())) {
//							temp.remove(m);
//						}
//					});
//				});
//
//				setMethodList.removeAll(temp);
//
//				Spliterator<Row> spliterator = Spliterators.spliteratorUnknownSize(partitions, 0);
//				Stream<Row> targetStream = StreamSupport.stream(spliterator, false);
//				targetStream.forEach(Row ->
//				{
//					if (Row != null) {
//						try {
//							objInstance = Class.forName(modelName).newInstance();
//							Object modelInstance = getModelInstance(Row, newGetColumnList, setMethodList, objInstance);
//							FactHandle fact = ksession.insert(modelInstance);
//							ksession.fireAllRules();
//							ksession.delete(fact);
//							Row row = RowFactory.create(callAllGetterMethods(modelInstance, setColumnList, getMethodList));
//							resRow.add(row);
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//				});
//				ksession.destroy();
//				ksession.dispose();
//				return resRow.iterator();
//			});
//
//			StructType silverSchema = (StructType) StructType.fromJson(droolsSchema);
//			DroolsDataset = spark.createDataFrame(DroolsRowRDD, silverSchema);
//		} catch (Exception ex) {
//			log.error("Exception found in executeDrools class " + ex.getMessage());
//			ex.printStackTrace();
//			throw ex;
//		}
//		return DroolsDataset;
//	}
//
//	public Dataset<Row> getValidData(SparkSession spark, FileSourceConfig fsConfig) throws Exception {
//		try {
//
//			Dataset<Row> validRecordDataSet = spark.sql(fsConfig.getProperty("drools.silver.table.query"))
//					.withColumn("LAST_UPDATE_DT", functions.current_timestamp())
//					.drop("ERROR_MESSAGE").drop("ERROR_TYPE");
//			return validRecordDataSet;
//		} catch (Exception ex) {
//			log.error("Exception found in getValidData class: " + ex.getMessage());
//			throw ex;
//		}
//	}
//
//	public Dataset<Row> getInvalidData(SparkSession spark, FileSourceConfig fsConfig) throws Exception {
//		try {
//			Dataset<Row> invalidRecordDataSet = spark.sql(fsConfig.getProperty("drools.error.table.query"))
//					.drop("ROW_START_DT").drop("ROW_END_DT")
//					.withColumn("VALIDATION_TYPE", functions.lit("Business Rules Management"))
//					.withColumn("LAST_UPDATE_DT", functions.current_timestamp());
//
//			return invalidRecordDataSet;
//		} catch (Exception ex) {
//			log.error("Exception found in getInvalidData class: " + ex.getMessage());
//			throw ex;
//		}
//	}
//	public  void replaceMetadataFile(String data,String FileLocation) throws Exception
//	{
//		FileWriter filewriter;
//		try {
//			File file = new File(FileLocation);
//
//			filewriter = new FileWriter(file,false);
//			BufferedWriter writer =  new BufferedWriter(filewriter);
//			writer.write(data.toString());
//			writer.close();
//
//			filewriter.close();
//		}catch (Exception e) {
//			e.printStackTrace();
//			throw e;
//		}
//	}
//}
