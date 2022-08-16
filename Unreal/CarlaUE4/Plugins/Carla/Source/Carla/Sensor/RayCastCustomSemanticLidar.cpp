// Copyright (c) 2020 Computer Vision Center (CVC) at the Universitat Autonoma
// de Barcelona (UAB).
//
// This work is licensed under the terms of the MIT license.
// For a copy, see <https://opensource.org/licenses/MIT>.

#include <PxScene.h>
#include <cmath>
#include "Carla.h"
#include "Carla/Actor/ActorBlueprintFunctionLibrary.h"
#include "Carla/Sensor/RayCastCustomSemanticLidar.h"
#include <carla/rpc/String.h>

#include <compiler/disable-ue4-macros.h>
#include "carla/geom/Math.h"
#include <compiler/enable-ue4-macros.h>

#include "DrawDebugHelpers.h"
#include "Engine/CollisionProfile.h"
#include "Runtime/Engine/Classes/Kismet/KismetMathLibrary.h"
#include "Runtime/Core/Public/Async/ParallelFor.h"
#include <fstream>

namespace crp = carla::rpc;

FActorDefinition ARayCastCustomSemanticLidar::GetSensorDefinition()
{
  return UActorBlueprintFunctionLibrary::MakeLidarDefinition(TEXT("ray_cast_semantic_zvision"));
}

ARayCastCustomSemanticLidar::ARayCastCustomSemanticLidar(const FObjectInitializer& ObjectInitializer)
  : Super(ObjectInitializer)
{
  PrimaryActorTick.bCanEverTick = true;
}

void ARayCastCustomSemanticLidar::Set(const FActorDescription &ActorDescription)
{
  Super::Set(ActorDescription);
  FLidarDescription LidarDescription;
  UActorBlueprintFunctionLibrary::SetLidar(ActorDescription, LidarDescription);
  Set(LidarDescription);
}

void ARayCastCustomSemanticLidar::Set(const FLidarDescription &LidarDescription)
{
  Description = LidarDescription;
  SemanticLidarData = FSemanticLidarData(Description.Channels);
  CreateLasers();
  PointsPerChannel.resize(Description.Channels);
}

void ARayCastCustomSemanticLidar::CreateLasers()
{
  const auto NumberOfLasers = int(Description.PointsPerSecond / Description.RotationFrequency);
  check(NumberOfLasers > 0u);
  // const float DeltaAngle = NumberOfLasers == 1u ? 0.f :
  //   (Description.UpperFovLimit - Description.LowerFovLimit) /
  //   static_cast<float>(NumberOfLasers - 1);
  // LaserAngles.Empty(NumberOfLasers);
  // for(auto i = 0u; i < NumberOfLasers; ++i)
  // {
  //   const float VerticalAngle =
  //       Description.UpperFovLimit - static_cast<float>(i) * DeltaAngle;
  //   LaserAngles.Emplace(VerticalAngle);
  // }

  LaserHorizontalAngles.Empty(NumberOfLasers);
  LaserVerticalAngles.Empty(NumberOfLasers);
  double tmp;
  std::string path = carla::rpc::FromFString(Description.LidarCalPath);
  std::ifstream in(path.c_str());

  if(Description.LidarType == 0){
    for(int i = 0; i < 6400; i++)
    {
        for(int j = 0; j <= 8; j++)
        {   
            in >> tmp;
            if(j == 1 || j == 3 || j == 5 || j == 7)
            {
                LaserHorizontalAngles.Emplace(tmp);
            }
            else if(j == 2 || j == 4 || j == 6 || j == 8)
            {
                LaserVerticalAngles.Emplace(tmp);
            }
        }
    }
  }else if(Description.LidarType == 1){
    // # 注释
    /**
    # file version:MLXS0001.cal_v0.0
    VERSION 0.1
    Mode MLXs_180
    1 -64.967 17.154 -20.368 13.765 9.715 12.600
    ...
    36000 -62.260 -9.410 -19.969 -11.854 9.756 -12.901
     */
    in >> tmp;
    in >> tmp;
    in >> tmp;
    for(int i = 0; i < 36000; i++)
    {
        for(int j = 0; j <= 6; j++)
        {   
            in >> tmp;
            if(j == 1 || j == 3 || j == 5)
            {
                LaserHorizontalAngles.Emplace(tmp);
            }
            else if(j == 2 || j == 4 || j == 6)
            {
                LaserVerticalAngles.Emplace(tmp);
            }
        }
    }
  }
  
}

void ARayCastCustomSemanticLidar::PostPhysTick(UWorld *World, ELevelTick TickType, float DeltaTime)
{
  TRACE_CPUPROFILER_EVENT_SCOPE(ARayCastCustomSemanticLidar::PostPhysTick);
  SimulateLidar(DeltaTime);

  {
    TRACE_CPUPROFILER_EVENT_SCOPE_STR("Send Stream");
    auto DataStream = GetDataStream(*this);
    DataStream.Send(*this, SemanticLidarData, DataStream.PopBufferFromPool());
  }
}

void ARayCastCustomSemanticLidar::SimulateLidar(const float DeltaTime)
{
  TRACE_CPUPROFILER_EVENT_SCOPE(ARayCastCustomSemanticLidar::SimulateLidar);
  const uint32 ChannelCount = Description.Channels;//Description.Channels;
  const uint32 PointsToScanWithOneLaser =
    FMath::RoundHalfFromZero(
        Description.PointsPerSecond * DeltaTime / float(ChannelCount));

  if (PointsToScanWithOneLaser <= 0)
  {
    UE_LOG(
        LogCarla,
        Warning,
        TEXT("%s: no points requested this frame, try increasing the number of points per second."),
        *GetName());
    return;
  }

  // check(ChannelCount == LaserAngles.Num());

  const float CurrentHorizontalAngle = carla::geom::Math::ToDegrees(
      SemanticLidarData.GetHorizontalAngle());
  const float AngleDistanceOfTick = Description.RotationFrequency * Description.HorizontalFov
      * DeltaTime;
  const float AngleDistanceOfLaserMeasure = AngleDistanceOfTick / PointsToScanWithOneLaser;

  ResetRecordedHits(ChannelCount, PointsToScanWithOneLaser);
  PreprocessRays(ChannelCount, PointsToScanWithOneLaser);

  GetWorld()->GetPhysicsScene()->GetPxScene()->lockRead();
  {
    TRACE_CPUPROFILER_EVENT_SCOPE(ParallelFor);
    ParallelFor(ChannelCount, [&](int32 idxChannel) {
      TRACE_CPUPROFILER_EVENT_SCOPE(ParallelForTask);

      FCollisionQueryParams TraceParams = FCollisionQueryParams(FName(TEXT("Laser_Trace")), true, this);
      TraceParams.bTraceComplex = true;
      TraceParams.bReturnPhysicalMaterial = false;

      for (auto idxPtsOneLaser = 0u; idxPtsOneLaser < PointsToScanWithOneLaser; idxPtsOneLaser++) {
        FHitResult HitResult;
        // const float VertAngle = LaserAngles[idxChannel];
        // const float HorizAngle = std::fmod(CurrentHorizontalAngle + AngleDistanceOfLaserMeasure
        //     * idxPtsOneLaser, Description.HorizontalFov) - Description.HorizontalFov / 2;

        const float VertAngle = LaserVerticalAngles[idxChannel*PointsToScanWithOneLaser + idxPtsOneLaser];
        const float HorizAngle = LaserHorizontalAngles[idxChannel*PointsToScanWithOneLaser + idxPtsOneLaser];
        const bool PreprocessResult = RayPreprocessCondition[idxChannel][idxPtsOneLaser];
        if (PreprocessResult && ShootLaser(VertAngle, HorizAngle, HitResult, TraceParams)) {
          WritePointAsync(idxChannel, HitResult);
        }
      };
    });
  }
  GetWorld()->GetPhysicsScene()->GetPxScene()->unlockRead();

  FTransform ActorTransf = GetTransform();
  ComputeAndSaveDetections(ActorTransf);

  const float HorizontalAngle = carla::geom::Math::ToRadians(
      std::fmod(CurrentHorizontalAngle + AngleDistanceOfTick, Description.HorizontalFov));
  SemanticLidarData.SetHorizontalAngle(HorizontalAngle);
}

void ARayCastCustomSemanticLidar::ResetRecordedHits(uint32_t Channels, uint32_t MaxPointsPerChannel) {
  RecordedHits.resize(Channels);

  for (auto& hits : RecordedHits) {
    hits.clear();
    hits.reserve(MaxPointsPerChannel);
  }
}

void ARayCastCustomSemanticLidar::PreprocessRays(uint32_t Channels, uint32_t MaxPointsPerChannel) {
  RayPreprocessCondition.resize(Channels);

  for (auto& conds : RayPreprocessCondition) {
    conds.clear();
    conds.resize(MaxPointsPerChannel);
    std::fill(conds.begin(), conds.end(), true);
  }
}

void ARayCastCustomSemanticLidar::WritePointAsync(uint32_t channel, FHitResult &detection) {
	TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);
  DEBUG_ASSERT(GetChannelCount() > channel);
  RecordedHits[channel].emplace_back(detection);
}

void ARayCastCustomSemanticLidar::ComputeAndSaveDetections(const FTransform& SensorTransform) {
	TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);
  for (auto idxChannel = 0u; idxChannel < Description.Channels; ++idxChannel)
    PointsPerChannel[idxChannel] = RecordedHits[idxChannel].size();
  SemanticLidarData.ResetMemory(PointsPerChannel);

  for (auto idxChannel = 0u; idxChannel < Description.Channels; ++idxChannel) {
    for (auto& hit : RecordedHits[idxChannel]) {
      FSemanticDetection detection;
      ComputeRawDetection(hit, SensorTransform, detection);
      SemanticLidarData.WritePointSync(detection);
    }
  }

  SemanticLidarData.WriteChannelCount(PointsPerChannel);
}

void ARayCastCustomSemanticLidar::ComputeRawDetection(const FHitResult& HitInfo, const FTransform& SensorTransf, FSemanticDetection& Detection) const
{
    const FVector HitPoint = HitInfo.ImpactPoint;
    Detection.point = SensorTransf.Inverse().TransformPosition(HitPoint);

    const FVector VecInc = - (HitPoint - SensorTransf.GetLocation()).GetSafeNormal();
    Detection.cos_inc_angle = FVector::DotProduct(VecInc, HitInfo.ImpactNormal);

    const FActorRegistry &Registry = GetEpisode().GetActorRegistry();

    const AActor* actor = HitInfo.Actor.Get();
    Detection.object_idx = 0;
    Detection.object_tag = static_cast<uint32_t>(HitInfo.Component->CustomDepthStencilValue);

    if (actor != nullptr) {

      const FCarlaActor* view = Registry.FindCarlaActor(actor);
      if(view)
        Detection.object_idx = view->GetActorId();

    }
    else {
      UE_LOG(LogCarla, Warning, TEXT("Actor not valid %p!!!!"), actor);
    }
}


bool ARayCastCustomSemanticLidar::ShootLaser(const float VerticalAngle, const float HorizontalAngle, FHitResult& HitResult, FCollisionQueryParams& TraceParams) const
{
  TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);

  FHitResult HitInfo(ForceInit);

  FTransform ActorTransf = GetTransform();
  FVector LidarBodyLoc = ActorTransf.GetLocation();
  FRotator LidarBodyRot = ActorTransf.Rotator();

  FRotator LaserRot (VerticalAngle, HorizontalAngle, 0);  // float InPitch, float InYaw, float InRoll
  FRotator ResultRot = UKismetMathLibrary::ComposeRotators(
    LaserRot,
    LidarBodyRot
  );

  const auto Range = Description.Range;
  FVector EndTrace = Range * UKismetMathLibrary::GetForwardVector(ResultRot) + LidarBodyLoc;

  GetWorld()->ParallelLineTraceSingleByChannel(
    HitInfo,
    LidarBodyLoc,
    EndTrace,
    ECC_GameTraceChannel2,
    TraceParams,
    FCollisionResponseParams::DefaultResponseParam
  );

  if (HitInfo.bBlockingHit) {
    HitResult = HitInfo;
    return true;
  } else {
    return false;
  }
}
