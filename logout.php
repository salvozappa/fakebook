<?php 
require_once 'config.php';
require_once 'php/Fakebook.php';

$fakebook = new Fakebook();

$fakebook->logout();

header("location: home.php");