use Getopt::Long;

# Get parameters - if not there then display help and exit
my %files = get_parameters();

# Load the patient file
my %patients = load_patient_file($files{"patient"});

# Load the inr file
my %inrs = load_inr_file($files{"inr"});

# print inrs for one patient
my $key = (keys %inrs)[0];

print $key , "\n";
for $href ( @{$inrs{$key}} ) {
    print "{ ";
    for $role ( keys %$href ) {
         print "$role=$href->{$role} ";
    }
    print "}\n";
}

sub get_parameters {
  my %files=();
  $files{"inr"} = "";
  $files{"patient"} = "";

  GetOptions ("inrfile=s"   => \$files{"inr"},   # string
              "patientfile=s"   => \$files{"patient"})   # string
  or die("Error in command line arguments\n");

  die "Required option inrFile!\n" unless $files{"inr"};
  die "Required option patientFile!\n" unless $files{"patient"};

  return %files;
}

sub load_patient_file {
  my $patientFile = @_[0];

  open my $handle, '<', $patientFile;
  chomp(my @lines = <$handle>);
  close $handle;

  my %treatments = ();
  my $line = 1;
  foreach $a (@lines) {
    my @row = split(/\t/,$a);
    my $UniqueTreatmentPlanID = @row[1];
    if($treatments{$UniqueTreatmentPlanID}) {
      print "Error detected - there is a duplicate ExpandedUniqueTreatmentPlanID: " , $UniqueTreatmentPlanID, " on row ", $line, "\n";
    } elsif(!$UniqueTreatmentPlanID){
      print "Error detected - there is a record without an ExpandedUniqueTreatmentPlanID", " on row ", $line, "\n";
    }
    $treatments{$UniqueTreatmentPlanID} = [@row];
    #print +@{$treatments{$UniqueTreatmentPlanID}},  "\n";
    $line++;
  }

  return %treatments;
}

sub load_inr_file {
  my $inrFile = @_[0];

  open my $handle, '<', $inrFile;
  chomp(my @lines = <$handle>);
  close $handle;

  my %inrs = ();
  my $line = 1;
  foreach $a (@lines) {
    my @row = split(/\t/,$a);
    my $UniqueTreatmentPlanID = @row[0];
    my %valuepair = { inr  => @row[2], date => @row[1] };
    if(!$inrs{$UniqueTreatmentPlanID}) {
      my @arr = ({ inr  => @row[2], date => @row[1] });
      $inrs{$UniqueTreatmentPlanID} = [@arr];
    } else {
      push ( @{$inrs{$UniqueTreatmentPlanID}}, { inr  => @row[2], date => @row[1] });
    }

    $line++;
  }

  return %inrs;
}
