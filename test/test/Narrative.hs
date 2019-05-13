{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Narrative
where

import            Data.Aeson
import            Data.Aeson.Types (Parser)

import            Control.Concurrent

import qualified  Data.ByteString.Char8 as BS
import qualified  Data.List as L
import qualified  Data.Map.Strict as M
import qualified  Data.Set as S

import            Data.Time.Calendar
import            Data.Time.Clock.POSIX
import            Data.Time.Clock
import            Data.Time.LocalTime

import            Test.QuickCheck
import            Test.QuickCheck.Monadic

import            Network.HTTP.Simple as HTTP
import            Network.HTTP.Client

runtest :: IO ()
runtest = do quickCheck prop


prop :: Messages -> Property
prop (Messages msgs) = monadicIO $ run $ do
  aggregateProperty msgs


-- Data

data Event = Click | Impression
  deriving (Eq)

newtype User = User String
  deriving (Eq, Show, Ord)

data Msg = MkMsg
  { msgHour       :: UTCTime
  , msgTimestamp  :: UTCTime
  , msgUser       :: User
  , msgEvent      :: Event
  }
  deriving (Eq, Show)

data AggregateMsg = MkAggregateMsg
  { aTheHour      :: UTCTime
  , aGetTimeStamp :: UTCTime    -- ^ Timestamp used by the GET query
  , aUsers        :: S.Set User
  , aNbClick      :: Int
  , aNbImpression :: Int
  }
  deriving (Show)




-- Compute the aggregated messages,.
aggregateMsg :: [Msg] -> [AggregateMsg]
aggregateMsg msgs =
  L.map aggregateOneGroup $ M.elems groups
  where
  groups = L.foldl' foldProc M.empty msgs
  foldProc m msg@(MkMsg h _ _ _) = M.insertWith (++) h [msg] m
  aggregateOneGroup msgs' =
    L.foldl' foldProc' (MkAggregateMsg zeroUTCTime zeroUTCTime S.empty 0 0) msgs'
    where
    foldProc' (MkAggregateMsg _ _ users nbClicks nbImpressions) (MkMsg h ts user event) =
      let (nbClicks', nbImpressions') =
            if event == Click
            then (nbClicks+1, nbImpressions)
            else (nbClicks, nbImpressions+1)
         -- we use one of the message timestamp as the GET timestamp (could improved - average ts)
      in MkAggregateMsg h ts (S.insert user users) nbClicks' nbImpressions'
  zeroUTCTime = UTCTime (fromGregorian 0 0 0) 0


-- generators

newtype Messages = Messages [Msg]
  deriving (Show)

msgsGen :: Gen [Msg]
msgsGen = do
  (nh::Int) <- choose (1, 10)                                   -- number of different hours
  (hours :: [UTCTime]) <-  vectorOf nh arbitrary
  let hours' = S.toList $ S.fromList $ L.map roundToHour hours  -- remove duplicates
  msgs <- mapM go hours'
  shuffle $ L.concat msgs

  where
  go h = do
    n <- choose (2, 20)  -- number of timestamps within the given hour
    msgs <- vectorOf n (msgGen h)
    return msgs

msgGen :: UTCTime -> Gen Msg
msgGen theHour = do
  s <- choose (0, 3599)
  let ts = addUTCTime (toEnum (s * 1000000000000)) theHour
  user <- arbitrary
  event <- arbitrary
  return $ MkMsg (roundToHour ts) ts user event


aggregateProperty :: [Msg] -> IO Bool
aggregateProperty msgs = do

  -- post all messages
  mapM_ postMessage msgs

  -- wait a bit
  threadDelay 500000

  -- for each aggregated message, query the server and check.
  bools <- mapM getAggregate aggregatedMsgs
  return $ L.all id bools
  where
  aggregatedMsgs = aggregateMsg msgs




-- Http request and checks

postMessage :: Msg -> IO ()
postMessage (MkMsg _ timestamp (User user) event) = do
  req <- parseRequest $ "POST http://localhost:9000/analytics"
  let ndt :: NominalDiffTime = utcTimeToPOSIXSeconds timestamp
  let ( millis::Integer) = (round $ realToFrac ndt) * 1000
  let req' = setQueryString [
                ("timestamp", Just (BS.pack $ show millis))
                , ("user", Just (BS.pack user))
                , ("event", Just (BS.pack $ show event))] req
  _ <- httpLBS req'
  return ()


data GetResult = MkGetResult
  { rNbUsers        :: Int
  , rNbClicks       :: Int
  , rNbImpressions  :: Int
  }
  deriving (Show)


getAggregate :: AggregateMsg -> IO Bool
getAggregate am@(MkAggregateMsg theHour getTimestamp users nbClicks nbImpressions) = do
  req <- parseRequest $ "GET http://localhost:9000/analytics"
  let ndt :: NominalDiffTime = utcTimeToPOSIXSeconds getTimestamp
  let (millis::Integer) = (round $ realToFrac ndt) * 1000
  let req' = setQueryString [("timestamp", Just (BS.pack $ show millis))] req
  response <- httpLBS req'
  let responseBody = getResponseBody response
  print responseBody
  case eitherDecode responseBody of
    Right rm@(MkGetResult nbUsers' nbClicks' nbImpressions') -> do
      Prelude.putStrLn "=============="
      print am
      print rm
      return $ S.size users == nbUsers' && nbClicks == nbClicks' && nbImpressions == nbImpressions'
    Left e -> do
      print e
      return False



-- instances

instance Arbitrary User where
  arbitrary = do
    (n::Int) <- choose (1, 10)
    return $ User("user" ++ show n)

instance Arbitrary Event where
  arbitrary = oneof [pure Click, pure Impression]


instance Arbitrary Messages where
  arbitrary = do
    msgs <- msgsGen
    return $ Messages msgs

instance Arbitrary UTCTime where
  arbitrary = do
    randomDay <- choose (1, 29) :: Gen Int
    randomMonth <- choose (1, 12) :: Gen Int
    randomYear <- choose (1970, 2345) :: Gen Integer
    randomTime <- choose (0, 86401) :: Gen Int
    return $ UTCTime (fromGregorian randomYear randomMonth randomDay) (fromIntegral randomTime)

instance Show Event where
  show Click = "click"
  show Impression = "impression"



instance FromJSON GetResult where
  parseJSON = withObject "Get Result" $ \o -> do
    rNbUsers              <- o .: "nbUsers" :: Parser Int
    rNbClicks             <- o .: "nbClicks" :: Parser Int
    rNbImpressions        <- o .: "nbImpressions" :: Parser Int
    return $ MkGetResult {..}


-- Misc


millisToUTC :: Integer -> UTCTime
millisToUTC t = posixSecondsToUTCTime $ (fromInteger t) / 1000


decodeEvent :: String -> Event
decodeEvent "click" = Click
decodeEvent "impression" = Impression

roundToHour :: UTCTime -> UTCTime
roundToHour (UTCTime d dt) =
  UTCTime d dt'
  where
  TimeOfDay h _ _ = timeToTimeOfDay dt
  dt' = timeOfDayToTime $ TimeOfDay h 0 0
