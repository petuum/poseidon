// Copyright (c) 2014, Sailing Lab
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the <ORGANIZATION> nor the names of its contributors
// may be used to endorse or promote products derived from this software
// without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//author: Pengtao Xie (pengtaox@cs.cmu.edu)


#include "dnn.h"
#include "util.h"
#include "dnn_utils.h"
#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <cmath>
#include <io/general_fstream.hpp>
#include <petuum_ps/util/high_resolution_timer.hpp>
#include <petuum_ps/thread/context.hpp>
//#include <petuum_ps_common/util/stats.hpp>

dnn::dnn(dnn_paras para,
         int client_id,
         int num_worker_threads,
         int staleness,
         int num_train_data) {

    num_layers=para.num_layers;
    num_units_ineach_layer=new int[num_layers];
    for(int i=0;i<num_layers;i++){
        num_units_ineach_layer[i]=para.num_units_ineach_layer[i];
    }
    num_epochs = para.epochs;
    stepsize=para.stepsize;
    size_minibatch=para.size_minibatch;
    this->num_worker_threads=num_worker_threads;
    process_barrier.reset(new boost::barrier(num_worker_threads));
    thread_counter=0;

    this->client_id = client_id;
    this->client_index = petuum::GlobalContext::get_worker_client_index(client_id);
    this->staleness=staleness;
    this->num_train_data=num_train_data;
    num_smps_evaluate=para.num_smps_evaluate;
    num_iters_evaluate=para.num_iters_evaluate;
    num_iters_print_stats = para.num_iters_print_stats;


    latest_model_version = 0;
    client_iteration_number = 0;

}


void dnn::reset_deltas(float*** delta_weights, float** delta_biases) {
  for(int l = 0; l < num_layers-1; l++){
    int dim1 = num_units_ineach_layer[l+1], dim2 = num_units_ineach_layer[l];
    for(int i = 0; i < dim1; i++) {
      memset(delta_weights[l][i],0,sizeof(float)*dim2);
    }
  }
  for(int l = 0; l < num_layers - 1; l++) {
    memset(delta_biases[l], 0, sizeof(float)*num_units_ineach_layer[l+1]);
  }
}


void dnn::buffer_in_model(mat* weights,
                          mat* biases,
                          int** rand_idxes_weight,
                          int* rand_idxes_bias) {

  // will send row requests
    for(int l = 0; l < num_layers-1; l++) {

      // ask
      int counter = 0;
        int dim1 = num_units_ineach_layer[l+1];
        for(int j = 0; j < dim1; j++) {
            int rnd_idx = rand_idxes_weight[l][j];
            weights[l].GetAsync(rnd_idx);
            counter++;
        }
        VLOG(20) << "Sent " << counter << " requests for weight table="  << l;

        // wait
        counter = 0;
        for(int j = 0; j < dim1; j++) {
          weights[l].WaitPendingAsyncGet();
          counter++;
          VLOG(20) << "Received " << counter <<  " row request replies for table=" << l;
        }
        VLOG(20) << "Received ALL row reply requests for table=" << l;
    }

    for(int l = 0; l < num_layers-1; l++) {
        int rnd_idx = rand_idxes_bias[l];
        // ask
        biases[rnd_idx].GetAsync(0);

        // wait
        biases[rnd_idx].WaitPendingAsyncGet();
    }

    /*
    // will wait for as many row requests replies
    for(int l = 0; l < num_layers-1; l++) {
      int dim1 = num_units_ineach_layer[l+1];
      for(int j = 0; j < dim1; j++) {
        weights[l].WaitPendingAsyncGet();
      }
      break;
    }

    for(int l = 0; l < num_layers-1; l++) {
      int rnd_idx = rand_idxes_bias[l];
      biases[rnd_idx].WaitPendingAsyncGet();
    }
    */

}



void dnn::read_from_storage(float*** local_weights,
                          float** local_biases,
                          mat* weights,
                          mat* biases,
                          int** rand_idxes_weight,
                          int* rand_idxes_bias,
                          int* min_model_version,
                          int* avg_model_version,
                          int* max_model_version) {

    petuum::RowAccessor row_acc;
    //fetch parameters from PS tables to local parameter buffers
    //(local_weights is the local copy of weight tables, local_biases is the local copy of bias tables)

    int num_params = 0; double version = 0.0;

    for(int l = 0; l < num_layers-1; l++) {
        int dim1 = num_units_ineach_layer[l+1], dim2 = num_units_ineach_layer[l];
        for(int j = 0; j < dim1; j++) {
            int rnd_idx = rand_idxes_weight[l][j];
            const auto& r = weights[l].Get<petuum::DenseRow<float> >(rnd_idx, &row_acc);

            *min_model_version = std::min(*min_model_version, row_acc.GetClientRow()->GetGlobalVersion());
            *max_model_version = std::max(*max_model_version, row_acc.GetClientRow()->GetGlobalVersion());
            version += row_acc.GetClientRow()->GetGlobalVersion();
            num_params += 1;

            for(int i = 0; i < dim2; i++) {
              local_weights[l][rnd_idx][i] = r[i];
            } // end for -- loop over dim2
        } // end for -- loop over dim1
    } // end for -- loop over layers of neural network

    for(int l = 0; l < num_layers-1; l++) {
        int rnd_idx = rand_idxes_bias[l];
        int dim = num_units_ineach_layer[rnd_idx+1];
        const auto& r = biases[rnd_idx].Get<petuum::DenseRow<float> >(0, &row_acc);

        *min_model_version = std::min(*min_model_version, row_acc.GetClientRow()->GetGlobalVersion());
        *max_model_version = std::max(*max_model_version, row_acc.GetClientRow()->GetGlobalVersion());
        version += row_acc.GetClientRow()->GetGlobalVersion();
        num_params += 1;

        for(int j = 0; j < dim; j++) {
          local_biases[rnd_idx][j] = r[j];
        } // end for -- loop over neurons in a layer
    } // end for -- loop over layers of a neural network

    *avg_model_version = (int) (version / num_params);
}


void dnn::sgd_mini_batch(int * idxes_batch,
                         mat* weights,
                         mat* biases,
                         float *** local_weights,
                         float ** local_biases ,
                         float *** delta_weights,
                         float ** delta_biases,
                         float ** z,
                         float ** delta,
                         int ** rand_idxes_weight,
                         int * rand_idxes_bias) {

    //delta_weights accumuluates the gradients of weight matrices, delta_biases
    //accumulates the gradients of bias vectors set delta_weights and
    //delta_biases buffer to zero
  reset_deltas(delta_weights, delta_biases);

  // buffer in the new rows asynchronously (wait after all the row requests have
  // been sent, not individually)
  petuum::HighResolutionTimer buffer_timer;
  buffer_in_model(weights, biases, rand_idxes_weight, rand_idxes_bias);
  VLOG(2) << "Buffering weights and biases took " << buffer_timer.elapsed()
          << " s. Iteration " << client_iteration_number
          << " version " << latest_model_version;


  // read the new rows from the process storage
  int32_t min_version = INT_MAX;
  int32_t max_version = INT_MIN;
  int32_t avg_version = 0;

  petuum::HighResolutionTimer read_local_weight_timer;
  read_from_storage(local_weights, local_biases,
                  weights, biases,
                  rand_idxes_weight, rand_idxes_bias,
                  &min_version, &avg_version, &max_version);
  VLOG(10) << "Reading weights and biases from process storage took " << read_local_weight_timer.elapsed() << " s.";

    VLOG(10) << "Min model version: " << min_version;
    VLOG(10) << "Max model version: " << max_version;
    VLOG(10) << "Average model version: " << avg_version;


    //compute gradient of the mini batch
    petuum::HighResolutionTimer mini_batch_timer;
    for(int i=0; i < size_minibatch; i++)
        compute_gradient_single_data(idxes_batch[i], local_weights,  local_biases, delta_weights, delta_biases, z,  delta );
    VLOG(2) << "SGD minibatch took " << mini_batch_timer.elapsed()
            << " s for " << size_minibatch << " records. "
            << "Iteration " << client_iteration_number
            << " version " << latest_model_version;


    STATS_APP_DEFINED_ACCUM_VAL_INC(mini_batch_timer.elapsed()); // used to test stats synchronization


    //update parameters
    petuum::HighResolutionTimer update_tables_timer;
    float coeff_update = -stepsize/size_minibatch;
    for(int l = 0; l < num_layers-1; l++) {
        int dim1 = num_units_ineach_layer[l+1], dim2 = num_units_ineach_layer[l];
        for(int j = 0; j < dim1; j++) {
            int rnd_idx = rand_idxes_weight[l][j];
            petuum::UpdateBatch<float> update_batch;
            for (int i = 0; i < dim2; ++i) {
                update_batch.Update(i, coeff_update * delta_weights[l][rnd_idx][i]);
            }
            weights[l].BatchInc(rnd_idx, update_batch, min_version);
        } // end for -- over dim1
    } // end for -- over layers of a neural network

    for(int l = 0; l < num_layers-1; l++) {
        int rnd_idx = rand_idxes_bias[l];
        int dim = num_units_ineach_layer[rnd_idx + 1];
        petuum::UpdateBatch<float> update_batch;
        for(int j = 0; j < dim; j++) {
          update_batch.Update(j, coeff_update * delta_biases[rnd_idx][j]);
        } // end for -- over neurons in a layer
        biases[rnd_idx].BatchInc(0, update_batch, min_version);
    } // end for -- over number layers of a neural network

    latest_model_version = min_version;

    VLOG(2) << "Batch Inc tables took " << update_tables_timer.elapsed() << " s";

} // end function - sgd mini batch



void dnn::compute_gradient_single_data(int idx_data,
                                       float *** local_weights,
                                       float ** local_biases,
                                       float *** delta_weights,
                                       float ** delta_biases,
                                       float ** z,
                                       float ** delta) {

  // z[0] is nothing but the input vector
    copy_vec(z[0], input_features[idx_data], num_units_ineach_layer[0]);

    //forward propagation
    for(int i = 1; i < num_layers; i++) {
      // modifies last argument, z[i]
      forward_activation(i-1, local_weights[i-1], local_biases[i-1], z[i-1], z[i]);
    }

    //backward propagation -- compute error in is the first step in back propagation

    // modifies argument 1: delta
    compute_error_output_layer(delta[num_layers-2], z[num_layers-1],idx_data);


    for(int l = num_layers-3; l>=0; l--) {
      // modified delta[l] -- the lower layer, last but one argument
      backward_error_computation(l, local_weights[l+1], z[l+1], delta[l], delta[l+1]);
    }


    //accumulate gradient of weights matrices and bias vectors
    for(int l=0;l<num_layers-1;l++){
        int dim1=num_units_ineach_layer[l+1], dim2=num_units_ineach_layer[l];
        for(int j=0;j<dim1;j++){
            for(int i=0;i<dim2;i++)
                delta_weights[l][j][i]+=delta[l][j]*z[l][i];
        }
    }
    for(int l=1;l<num_layers;l++){
        int dim=num_units_ineach_layer[l];
        for(int j=0;j<dim;j++)
            delta_biases[l-1][j]+=delta[l-1][j];
    }

}




void dnn::forward_activation(int index_lower_layer,
                             float ** local_weights,
                             float * local_bias,
                             float * visible,
                             float * hidden) {

    int num_units_hidden=num_units_ineach_layer[index_lower_layer+1];
    int num_units_visible=num_units_ineach_layer[index_lower_layer];

    // hidden is updated
    matrix_vector_multiply(local_weights, visible, hidden, num_units_hidden, num_units_visible);

    add_vector(hidden, local_bias, num_units_hidden);

    if(index_lower_layer < num_layers-2) {
      activate_logistic(hidden, num_units_hidden);
    }
    else if(index_lower_layer == num_layers-2) {
      log2ori(hidden,num_units_hidden );
    }

}


//compute error at output layer
void dnn::compute_error_output_layer(float * error_output_layer,
                                     float * activation_output_layer,
                                     int idx_data) {

  int num_units_output_layer=num_units_ineach_layer[num_layers-1];
  int label=output_labels[idx_data];
  for(int k=0;k<num_units_output_layer;k++){
    if(label == k) {
      // if label is k, and the activation at last layer is 1, then there is no error.
      // other wise error is negative, asking you to increase the value?
      error_output_layer[k]=activation_output_layer[k]-1; // output should be 1
    } else {
      // for all other than label output should be 0,
      error_output_layer[k]=activation_output_layer[k];
    }
  }
}


void dnn::backward_error_computation(int index_lower_index,
                                     float ** local_weights,
                                     float * activation,
                                     float * error_lower_layer,
                                     float * error_higher_layer) {

    int num_j=num_units_ineach_layer[index_lower_index+1];
    int num_k=num_units_ineach_layer[index_lower_index+2];
    memset(error_lower_layer, 0, sizeof(float)* num_j);
    for(int k=0;k<num_k;k++){
        for(int j=0;j<num_j;j++)
            error_lower_layer[j]+=local_weights[k][j]*error_higher_layer[k];
    }
    for(int j=0;j<num_j;j++){
        error_lower_layer[j]*=activation[j]*(1-activation[j]);
    }
}


void dnn::train(mat * weights,
                mat * biases) {

    //z stores forward activations, delta stores backward errors
    //allocate z and delta buffers
    float ** z=new float*[num_layers];
    for(int i=0;i<num_layers;i++)
        z[i]=new float[num_units_ineach_layer[i]];

    float ** delta=new float*[num_layers-1];
    for(int i=0;i<num_layers-1;i++)
        delta[i]=new float[num_units_ineach_layer[i+1]];

    //each iteration, we fetch the prameters from the PS table to local parameter buffers
    //local_weights is the local copy of weight matrices and local_biases is the local copy of bias vectors
    //create parameter buffer
    float *** local_weights=new float **[num_layers-1];
    for(int l=0;l<num_layers-1;l++){
        int dim1=num_units_ineach_layer[l+1], dim2=num_units_ineach_layer[l];
        local_weights[l]=new float*[dim1];
        for(int i=0;i<dim1;i++){
            local_weights[l][i]=new float[dim2];
            memset(local_weights[l][i],0,sizeof(float)*dim2);
        }
    }
    float ** local_biases=new float*[num_layers-1];
    for(int l=0;l<num_layers-1;l++){
        local_biases[l]=new float[num_units_ineach_layer[l+1]];
        memset(local_biases[l],0,sizeof(float)*num_units_ineach_layer[l+1]);
    }

    //delta_weights stores the gradient of weight matrices and delta_biases stores the gradient of bias vectors
    float *** delta_weights=new float **[num_layers-1];
    for(int l=0;l<num_layers-1;l++){
        int dim1=num_units_ineach_layer[l+1], dim2=num_units_ineach_layer[l];
        delta_weights[l]=new float*[dim1];
        for(int i=0;i<dim1;i++){
            delta_weights[l][i]=new float[dim2];
            memset(delta_weights[l][i],0,sizeof(float)*dim2);
        }
    }
    float ** delta_biases=new float*[num_layers-1];
    for(int l=0;l<num_layers-1;l++){
        delta_biases[l]=new float[num_units_ineach_layer[l+1]];
        memset(delta_biases[l],0,sizeof(float)*num_units_ineach_layer[l+1]);
    } // until now we are just creating a bunch of matrices in the heap (thread local) to store
    // the different variables (no get/set to local buffers are not processed yet.).

    int * idxes_batch = new int[size_minibatch];

    // epoch by definition is the number of passes over the ENTIRE data.
    // If each thread processes <size_minibatch> data, and there are <n> threads,
    // then we need (num_train_data/n/size_minibatch), iterations to complete one epoch
    int inner_iter = std::max(num_train_data / num_worker_threads /size_minibatch, 1);
    if((*thread_id) == 0) {
        VLOG(5) << "Value of inner_iter = " << inner_iter;
    }

    srand (time(NULL));
    int it=0;

    //randomly permute the row indexes to reduce thread contention of tables
    int ** rand_idxes_weight=new int*[num_layers-1];
    for(int l=0;l<num_layers-1;l++)
    {
        int dim=num_units_ineach_layer[l+1];
        rand_idxes_weight[l]=new int[dim];
        std::vector<int> output_idx_perm;
        for(int i=0;i<dim;i++)
            output_idx_perm.push_back(i);
        std::random_shuffle ( output_idx_perm.begin(), output_idx_perm.end(), myrandom);
        for(int i=0;i<dim;i++)
            rand_idxes_weight[l][i]=output_idx_perm[i];
    }

    int * rand_idxes_bias=new int[num_layers-1];
    {
        std::vector<int> output_idx_perm;
        for(int i=0;i<num_layers-1;i++)
            output_idx_perm.push_back(i);
        std::random_shuffle ( output_idx_perm.begin(), output_idx_perm.end(), myrandom);
        for(int i=0;i<num_layers-1;i++)
            rand_idxes_bias[i]=output_idx_perm[i];
    } // finish computing random permutation indices


    // Star the actual training process
    all_the_way_from_start.restart();
    for(int iter=0;iter<num_epochs;iter++){
        for(int i=0;i<inner_iter;i++) {
            //sample mini batch
            rand_init_vec_int(idxes_batch, size_minibatch, num_train_data);
            //run sgd
            sgd_mini_batch(idxes_batch, weights, biases, local_weights,  local_biases, delta_weights,  delta_biases, z,  delta, rand_idxes_weight,rand_idxes_bias);

            // Advance Parameter Server iteration
            petuum::PSTableGroup::Clock();

            it++;
            client_iteration_number++;

            //evalutate objective function

            //if(it % num_iters_evaluate == 0 && client_index == 0 && (*thread_id)==0) {
            if(it % num_iters_evaluate == 0 && (*thread_id)==0) {

                petuum::RowAccessor row_acc;
                //fetch parameters
                for(int l=0;l<num_layers-1;l++) {
                    int dim1=num_units_ineach_layer[l+1], dim2=num_units_ineach_layer[l];
                    for(int j = 0; j < dim1; j++) {
                        const auto& r = weights[l].Get<petuum::DenseRow<float> >(j, &row_acc);
                        for(int i=0;i<dim2;i++) {
                            local_weights[l][j][i]=r[i];
                        }
                    }
                }

                for(int l=0;l<num_layers-1;l++) {
                    int dim=num_units_ineach_layer[l+1];
                    const auto& r = biases[l].Get<petuum::DenseRow<float> >(0, &row_acc);
                    for(int j=0;j<dim;j++)
                        local_biases[l][j]=r[j];
                }

                //float loss = compute_loss(local_weights, local_biases);

                float prediction_error = compute_prediction_loss(local_weights, local_biases);

                if((*thread_id)==0) {
                  std::cout<< "client " << client_id << " worker " << (*thread_id) << " iter " << it
                           << " loss is " << 100.0 * prediction_error << " % "
                           << " version " << latest_model_version
                           << " time " << all_the_way_from_start.elapsed() << " s."
                           << std::endl;
                }

            }

            /*
            if(it % num_iters_print_stats == 0) {
              VLOG(2) << "Synchronize stats from thread: " << (*thread_id);
              STATS_SYNCHRONIZE();

              if((*thread_id) == 0) {
                // print the stats for all client, only from one thread;
                std::cout << "Invoke stats print at iteration: " << it << std::endl;
                STATS_PRINT();
              }
            }
            */ // disable stats print for now
        }
    }
    //release data
    delete []idxes_batch;
    for(int i=0;i<num_layers-1;i++)
        delete[]delta[i];
    delete[]delta;
    for(int i=0;i<num_layers;i++)
        delete[]z[i];
    delete []z;

    //release parameter buffer
    for(int l=0;l<num_layers-1;l++){
        int dim1=num_units_ineach_layer[l+1];
        for(int i=0;i<dim1;i++)
            delete []local_weights[l][i];
        delete[]local_weights[l];
    }
    delete[]local_weights;

    for(int l=0;l<num_layers-1;l++)
        delete []local_biases[l];
    delete []local_biases;

    for(int l=0;l<num_layers-1;l++)
    {
        int dim1=num_units_ineach_layer[l+1];
        for(int i=0;i<dim1;i++)
            delete []delta_weights[l][i];
        delete[]delta_weights[l];
    }
    delete[]delta_weights;
    for(int l=0;l<num_layers-1;l++)
        delete []delta_biases[l];
    delete []delta_biases;
}



float dnn::compute_loss(float *** weights, float ** biases) {

    float ** z=new float*[num_layers];
    for(int i=0;i<num_layers;i++)
        z[i]=new float[num_units_ineach_layer[i]];
    double loss=0;
    int cnt=0;

    double prediction_loss = 0.0;

    for(int smp=0; smp < num_train_data;smp++) {

        if(((rand()%100000)/100000.0)>(num_smps_evaluate*1.0/num_train_data))
            continue;

        //copy the first layer
        copy_vec(z[0], input_features[smp], num_units_ineach_layer[0]);

        //forward propagation
        for(int i = 1; i < num_layers; i++)
            forward_activation(i-1, weights[i-1], biases[i-1], z[i-1], z[i]);

        //compute cross entropy loss
        loss+=compute_cross_entropy_loss(z[num_layers-1], smp);
        prediction_loss += compute_zero_one_loss(z[num_layers-1], num_units_ineach_layer[num_layers-1], smp);
        cnt++;
    }

    loss/=cnt;

    prediction_loss /= cnt; // average loss over all samples
    std::cout << "Prediction error = " << prediction_loss;

    for(int i=0; i < num_layers; i++) {
      delete[]z[i];
    }
    delete[]z;
    return loss;
}


float dnn::compute_prediction_loss(float *** weights, float ** biases) {

    float ** z=new float*[num_layers];
    for(int i=0;i<num_layers;i++) {
      z[i]=new float[num_units_ineach_layer[i]];
    }

    int cnt=0;

    double prediction_loss = 0.0;

    for(int smp=0; smp < num_train_data;smp++) {

        if(((rand()%100000)/100000.0)>(num_smps_evaluate*1.0/num_train_data))
            continue;

        //copy the first layer
        copy_vec(z[0], input_features[smp], num_units_ineach_layer[0]);

        //forward propagation
        for(int i = 1; i < num_layers; i++) {
          forward_activation(i-1, weights[i-1], biases[i-1], z[i-1], z[i]);
        }

        //compute prediction loss
        prediction_loss += compute_zero_one_loss(z[num_layers-1], num_units_ineach_layer[num_layers-1], smp);
        cnt++;
    }

    prediction_loss /= cnt; // average loss over all samples

    for(int i=0; i < num_layers; i++) {
      delete[]z[i];
    }
    delete[]z;
    return prediction_loss;
}





float dnn::compute_zero_one_loss(float* output, int D, int idx_data) {
  int label = output_labels[idx_data];

  float maxv = -1.0;
  int maxid = -1;
  for(int i = 0; i < D; i++) {
    if(output[i] > maxv) {
      maxv = output[i];
      maxid = i;
    }
  }
  return (maxid == label) ? 0.0 : 1.0;
}


float dnn::compute_cross_entropy_loss(float * output,
                                      int idx_data) {

    float los=0;
    int label=output_labels[idx_data];
    if(std::abs(output[label])<1e-10)
        output[label]=1e-10;
    los-=log(output[label]);
    return los;
}


void dnn::save_model(mat * weights,
                     mat *biases,
                     const char * mdl_weight_file,
                     const char * mdl_bias_file) {

    //save weight matrices
    //std::ofstream outfile;
    //outfile.open(mdl_weight_file);
    petuum::io::ofstream outfile(mdl_weight_file);

    petuum::RowAccessor row_acc;
    for(int l=0;l<num_layers-1;l++){
        int dim1=num_units_ineach_layer[l+1], dim2=num_units_ineach_layer[l];
        for(int j=0;j<dim1;j++){
            const auto& r = weights[l].Get<petuum::DenseRow<float> >(j, &row_acc);
            //     weights[l].Get(j, &row_acc);
            //     const petuum::DenseRow<float>& r = row_acc.Get<petuum::DenseRow<float> >();
            for(int i=0;i<dim2;i++){
                outfile<<r[i]<<" ";
            }
            outfile<<std::endl;
        }
    }
    outfile.close();

    //save bias vectors
    //outfile.open(mdl_bias_file);
    petuum::io::ofstream outfile2(mdl_bias_file);

    for(int l=0;l<num_layers-1;l++){
        int dim=num_units_ineach_layer[l+1];
        const auto& r = biases[l].Get<petuum::DenseRow<float> >(0, &row_acc);
        //    biases[l].Get(0, &row_acc);
        //    const petuum::DenseRow<float>& r = row_acc.Get<petuum::DenseRow<float> >();
        for(int j=0;j<dim;j++){
            outfile2<<r[j]<<" ";
        }
        outfile2<<std::endl;
    }
    outfile2.close();
}


void dnn::load_data(char * data_file) {
    int feadim=num_units_ineach_layer[0];

    input_features=new float*[num_train_data];
    for(int i=0;i<num_train_data;i++){
        input_features[i]=new float[feadim];
    }
    output_labels=new int[num_train_data];
    //std::ifstream infile;
    //infile.open(data_file);
    petuum::io::ifstream infile(data_file);

    for(int i=0;i<num_train_data;i++){
        infile>>output_labels[i];
        for(int j=0;j<feadim;j++)
            infile>>input_features[i][j];
    }
    infile.close();
}


void dnn::init_paras(mat *weights,
                     mat *biases ) {

    //init weights and biases randomly
    VLOG(0) << "Starting parameter initialization.";
    for(int i=0;i<num_layers-1;i++){
        int num_rows=num_units_ineach_layer[i+1];
        int num_cols=num_units_ineach_layer[i];
        for(int j=0;j<num_rows;j++){
            petuum::UpdateBatch<float> update_batch;
            for(int k=0;k<num_cols;k++){
                float tmp = randfloat();
                update_batch.Update(k, tmp);
            }
            {
                weights[i].BatchInc(j,update_batch);
            }
        }
        {
            petuum::UpdateBatch<float> update_batch;
            for(int j=0;j<num_rows;j++) {
                float tmp = randfloat();
                update_batch.Update(j, tmp);
            }
            {
                biases[i].BatchInc(0,update_batch);
            }
        }
    }
    VLOG(0) << "Finished parameter initialization.";
}


// run the whole learning process of DNN
void dnn::run(std::string model_weight_file,
              std::string model_bias_file) {

    // assign id to threads
    if (!thread_id.get()) {
        thread_id.reset(new int(thread_counter++));
    }

    // get access to tables
    petuum::PSTableGroup::RegisterThread();
    mat *weights= new mat[num_layers-1];
    mat *biases=new mat[num_layers-1];
    for(int i=0;i<num_layers-1;i++){
        // GetTableorDies returns a pointer to AbstractClientTable (raajay: more precisely, it is
        // a pointer to abstract_table_group_->tables_[table_id]. The pointer is recast as a pointer to
        // AbstractClientTable)
        weights[i]=petuum::PSTableGroup::GetTableOrDie<float>(i);
        biases[i]=petuum::PSTableGroup::GetTableOrDie<float>(i+num_layers-1);
    }

    // Run additional iterations to let stale values finish propagating
    for (int iter = 0; iter < staleness; ++iter) {
        petuum::PSTableGroup::Clock();
    }

    // initialize parameters; only one client and one thread does the init.
    // Essentially, the tables are static objects (one per client). Thus, there
    // is an abstract_table_group_->tables static variable in each
    // client.However, each client is responsible only for a sub-set of
    // parameters in each table.
    if (client_index == 0 && (*thread_id) == 0) {
        std::cout<<"init parameters"<<std::endl;
        petuum::HighResolutionTimer init_paras_timer;
        init_paras(weights, biases);
        std::cout<<"init parameters done"<<std::endl;
        VLOG(2) << "init parameters took " << init_paras_timer.elapsed() << " seconds.";
    }
    process_barrier->wait();

    // do DNN training
    if (client_index == 0 && (*thread_id) == 0) {
      std::cout<<"training starts"<<std::endl;
    }
    train(weights, biases);

    // Run additional iterations to let stale values finish propagating
    for (int iter = 0; iter < staleness; ++iter) {
        petuum::PSTableGroup::Clock();
    }

    //save model
    if(client_index == 0 && (*thread_id) ==0 )
    {
        save_model(weights, biases, model_weight_file.c_str(), model_bias_file.c_str());
    }

    delete[]weights;
    delete[]biases;
    petuum::PSTableGroup::DeregisterThread();
}
