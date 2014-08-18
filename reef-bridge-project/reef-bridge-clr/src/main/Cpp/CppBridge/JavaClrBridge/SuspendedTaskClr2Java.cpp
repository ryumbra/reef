/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Driver
		{
			namespace Bridge
			{
				ref class ManagedLog
				{
				internal:
					static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
				};

				SuspendedTaskClr2Java::SuspendedTaskClr2Java(JNIEnv *env, jobject jobjectSuspendedTask)
				{
					ManagedLog::LOGGER->LogStart("SuspendedTaskClr2Java::SuspendedTaskClr2Java");
					pin_ptr<JavaVM*> pJavaVm = &_jvm;
					int gotVm = env -> GetJavaVM(pJavaVm);
					_jobjectSuspendedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectSuspendedTask));

					jclass jclassSuspendedTask = env->GetObjectClass (_jobjectSuspendedTask);
					jfieldID jidTaskId = env->GetFieldID(jclassSuspendedTask, "taskId", "Ljava/lang/String;");
					_jstringId = (jstring)env->GetObjectField(_jobjectSuspendedTask, jidTaskId);
					_jstringId = reinterpret_cast<jstring>(env->NewGlobalRef(_jstringId));
					ManagedLog::LOGGER->LogStop("SuspendedTaskClr2Java::SuspendedTaskClr2Java");
				}

				IActiveContextClr2Java^ SuspendedTaskClr2Java::GetActiveContext()
				{
					ManagedLog::LOGGER->LogStart("SuspendedTaskClr2Java::GetActiveContext");
					JNIEnv *env = RetrieveEnv(_jvm);

					jclass jclassSuspendedTask = env->GetObjectClass (_jobjectSuspendedTask);
					jfieldID jidActiveContext = env->GetFieldID(jclassSuspendedTask, "jactiveContext", "Lcom/microsoft/reef/javabridge/ActiveContextBridge;");
					jobject jobjectActiveContext = env->GetObjectField(_jobjectSuspendedTask, jidActiveContext);
					ManagedLog::LOGGER->LogStop("SuspendedTaskClr2Java::GetActiveContext");
					return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
				}

				String^ SuspendedTaskClr2Java::GetId()
				{
					ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::GetId");
					JNIEnv *env = RetrieveEnv(_jvm);
					return ManagedStringFromJavaString(env, _jstringId);
				}

				array<byte>^ SuspendedTaskClr2Java::Get()
				{
					ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::Get");
					JNIEnv *env = RetrieveEnv(_jvm);
					jclass jclassSuspendedTask = env->GetObjectClass (_jobjectSuspendedTask);
					jmethodID jmidGet = env->GetMethodID(jclassSuspendedTask, "get", "()[B");

					if(jmidGet == NULL)
					{
						ManagedLog::LOGGER->Log("jmidGet is NULL");
						return nullptr;
					}
					jbyteArray jMessage = (jbyteArray) env->CallObjectMethod(_jobjectSuspendedTask, jmidGet);
					return ManagedByteArrayFromJavaByteArray(env, jMessage);
				}
				
				void SuspendedTaskClr2Java::OnError(String^ message)
				{
					ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::OnError");
					JNIEnv *env = RetrieveEnv(_jvm);	
					HandleClr2JavaError(env, message, _jobjectSuspendedTask);
				}
			}
		}
	}
}