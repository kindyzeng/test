/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm_test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Date;
import java.util.Map;
import java.util.Random;

import model.Point_Mdl;


public class RandomPointSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;

  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    int id =(int)(Math.random() * 100000); 
    double X = Math.random()*360-180;
    double Y = Math.random()*180-90;
    com.esri.core.geometry.Point point = new com.esri.core.geometry.Point(X,Y);
//    Point_Mdl point_mdl =new Point_Mdl(id,point,new Date());
    Point_Mdl point_mdl =new Point_Mdl(id,point);
    System.out.println("����������������������������������������"+point_mdl.get_id());
    _collector.emit(new Values(point_mdl));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
	  System.out.println("SB��");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("point"));
  }

}