<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

if (!isset($_GET['id'])) {
	$fakebook->error(4);	// Missing parameters
}

$uid = $_GET['id'];

$fakebook = new Fakebook();

$myId = $fakebook->getLoggedUserId();

if (!$myId) {
	header("location: index.php");
}

if ($fakebook->addFriendRequest($uid)) {
	$fakebook->ok(1);		// request sent
} else {
	$fakebook->error(6); 	// they are already friends
}