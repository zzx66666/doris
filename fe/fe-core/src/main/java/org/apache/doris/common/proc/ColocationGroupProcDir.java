// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.proc;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.Tag;
import org.apache.doris.common.Config;
org.apache.doris.cloud.system.CloudSystemInfoService
org.apache.doris.qe.ConnectContext
com.google.common.hash.Hashing
java.util.stream.Collectors
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/*
 * show proc "/colocation_group";
 */
public class ColocationGroupProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("GroupId").add("GroupName").add("TableIds")
            .add("BucketsNum").add("ReplicaAllocation").add("DistCols").add("IsStable")
            .add("ErrorMsg").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String groupIdStr) throws AnalysisException {
        String[] parts = groupIdStr.split("\\.");
        if (parts.length != 2) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        long dbId = -1;
        long grpId = -1;
        try {
            dbId = Long.valueOf(parts[0]);
            grpId = Long.valueOf(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        GroupId groupId = new GroupId(dbId, grpId);
        ColocateTableIndex index = Env.getCurrentColocateIndex();
         // ================== 修改开始 ==================
         //仿照CloudReplica.java中的getColocatedBeId函数进行修改，主要是寻找云模式下的信息进行返回
        
        // 1. 判断是否是云模式 (Cloud Mode)
        //Config类中有配置信息，查阅为isCloudMode
        /**
         *   public static boolean isCloudMode() {
                return deploy_mode.equals("cloud") || !cloud_unique_id.isEmpty();
             }
         */
        if (Config.isCloudMode()) {
            // 获取云模式下的系统信息服务
            CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
            // 获取当前 Session 的 ClusterID 
            String clusterId = ConnectContext.get().getCloudClusterId();

            // 2. 获取该 Cluster 下所有可用的 Backend (参考了 CloudReplica 的逻辑)
            List<Backend> bes = infoService.getBackendsByClusterId(clusterId).stream()
                    .filter(be -> be.isQueryAvailable()).collect(Collectors.toList());
            
            // 简单处理：如果没有 BE，抛异常或返回空
            if (bes.isEmpty()) {
                 return new ColocationGroupBackendSeqsProcNode(new HashMap<>());
            }

            // 3. 准备哈希计算
            // 必须知道这个 Group 有多少个 Bucket (Tablets)。从 Index 的 Schema 里取
            int bucketsNum = index.getBackendsPerBucketSeq(groupId).size(); 
            // 注意：如果上面这行在云模式下取不到大小，可能需要 index.getSchema(groupId).getBuckets() 
            // 如果报错，尝试用： int bucketsNum = index.getGroupSchema(groupId).getBuckets();

            HashCode hashCode = Hashing.murmur3_128().hashLong(groupId.grpId);
            List<List<Long>> seqs = new ArrayList<>();

            // 4. 循环计算每一个 Bucket 应该在哪个 BE 上 (核心算法抄自 CloudReplica)
            for (int i = 0; i < bucketsNum; i++) {
                // CloudReplica 中的核心 Hash 算法：
                // long index = getIndexByBeNum(hashCode.asLong() + idx, availableBes.size());
                // 注意：这里 bes 我们假设都是 available 的，简化处理
                long hashIndex = (hashCode.asLong() + i) % bes.size();
                if (hashIndex < 0) {
                    hashIndex += bes.size();
                }
                
                Backend pickedBe = bes.get((int) hashIndex);
                
                List<Long> beIds = new ArrayList<>();
                beIds.add(pickedBe.getId());
                seqs.add(beIds);
            }

            // 5. 构造返回结果
            Map<Tag, List<List<Long>>> beSeqs = new HashMap<>();
            beSeqs.put(Tag.DEFAULT_TAG, seqs); 
            return new ColocationGroupBackendSeqsProcNode(beSeqs);
        }
        
        // ================== 修改结束 ==================

        // 原有逻辑 (非云模式)
        Map<Tag, List<List<Long>>> beSeqs = index.getBackendsPerBucketSeq(groupId);
        return new ColocationGroupBackendSeqsProcNode(beSeqs);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        List<List<String>> infos = index.getInfos();
        result.setRows(infos);
        return result;
    }
}
