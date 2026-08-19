
function outputJavascriptValidation(requiredElementsName,requiredElementsText) {

	var errormessage = "";
        for (var x=0; x < requiredElementsName.length; x++) {

            var element = $("[name='"+requiredElementsName[x]+"']");
            if (element.is('input')) {

                if (element.attr('type') == 'text' || element.attr('type') == 'hidden' || element.attr('type') == 'file') {
                    if (element.val() == '') errormessage += "\n\n" + requiredElementsText[x];
                } else if (element.attr('type') == 'radio') {
                    if ($('[name='+requiredElementsName[x]+']:checked').val() == undefined) errormessage += "\n\n" + requiredElementsText[x];
                } else if (element.attr('type') == 'checkbox') {
                    if ($('[name='+requiredElementsName[x]+']:checked').length == 0) errormessage += "\n\n" + requiredElementsText[x];
                } else {
                    errormessage += "\n\nUnknown input field type - " + requiredElementsText[x];
                }

            } else if (element.is('textarea')) {
                if (element.val() == '') errormessage += "\n\n" + requiredElementsText[x];
            } else if (element.is('select')) {
                if (element.val() == '') errormessage += "\n\n" + requiredElementsText[x];
            } else {
                errormessage += "\n\nUnknown element type - " + requiredElementsText[x];
            }

        }

        return errormessage;
        
}

function WithoutContent(ss) {
	if(ss.length > 0) {return false;}
	return true;
}

function checkCountry(origin, type) {

    type = typeof(type) != 'undefined' ? type : 'change';

    switch (origin) {

        case 'US':
            $('#province_row').hide();
            $('#postal_row').show();
            $('#us_state_row').show();
            if (type == 'change') {
                $('#province').val('');
                $('#state, #postal').val('');
                $('#us_state').focus();
            }
            break;


        case 'CA':
            $('#us_state_row').hide();
            $('#postal_row').show();
            $('#province_row').show();
            if (type == 'change') {
                $('#us_state').val('');
                $('#state, #postal').val('');
                $('#province').focus();
            }
            break;


        case 'GB':
        case 'DE':
        case 'CH':
            $('#province_row, #us_state_row').hide();
            $('#postal_row').show();
            if (type == 'change') {
                $('#province, #us_state').val('');
                $('#state').val('other');
                $('#postal').val('');
            }
            break;


        default:
            $('#province_row, #us_state_row, #postal_row').hide();
            if (type == 'change') {
                $('#province, #us_state').val('');
                $('#postal').val('N/A');
                $('#state').val('other');
            }

    }


    // Send resize message to parent
    if (typeof parentURL != 'undefined') {
        var bodyWidth = $('body').width();
        var bodyHeight = $('body').height();
        message = {action:'resize',data:{width:bodyWidth,height:bodyHeight}};
        $.postMessage(JSON.stringify(message),parentURL);
    }

}


function get_cookie(Name) {
	var search = Name + "="
	var returnvalue = "";
	if (document.cookie.length > 0) {
		offset = document.cookie.indexOf(search)
		if (offset != -1) {
			offset += search.length
			end = document.cookie.indexOf(";", offset);
			if (end == -1) end = document.cookie.length;
				returnvalue=unescape(document.cookie.substring(offset, end))
		}
	}
	return returnvalue;
}


function eatCookie () {
	document.cookie='cookietest=pentaho; expires=Thu, 2 Aug 1973 20:47:11 UTC; path=/; domain=.pentaho.com';
}


function checkCookies() {
	document.cookie='cookietest=pentaho; expires=Thu, 2 Aug 2020 20:47:11 UTC; path=/; domain=.pentaho.com';
	var hungry = get_cookie("cookietest");
	if(hungry == '') {
		alert(formvalid["no-cookies-warn"]);
	}
	return hungry;
}

function CheckRequiredFields() {
	var errormessage = "";

        if (typeof window.requiredFields == 'undefined') {
            var requiredFields = requiredElements();
        } else {
            requiredFields = window.requiredFields;
        }

	errormessage += outputJavascriptValidation(requiredFields.requiredElementsName,requiredFields.requiredElementsText);

// Put field checks above this point.
	if(errormessage.length > 2) {
		alert(formvalid["fields-not-completed"] + errormessage);
		
		return false;
	}

	// FF 2.0.0.6 refresh bug fix attempt
	if(document.getElementById('state').value == "other") { // this value is set manually - not translated
		if((document.getElementById('country').value == "United States") || (document.getElementById('country').value == "Canada")) {
			checkCountry(document.getElementById('country').value);
			CheckRequiredFields();
			return false;
		}
	}
	
	// add cookie if successful submit
//	document.cookie='assetGate=4rtfgb9i8ujhyrfg; expires=Thu, 2 Aug 2020 20:47:11 UTC; path=/; domain=.pentaho.com';
	$.cookie('assetGate','4rtfgb9i8ujhyrfg', {expires:'Thu, 2 Aug 2020 20:47:11 UTC', path:'/', domain:window.location.hostname.replace(/^[a-z0-9]+/i,'')});
	
	return true;
}

function handleType(currTypeVal) {
	// get special field names for sf values
	var sfFieldVal = specialFieldNames();
	
	if(currTypeVal == "other") {
		// show other input field for this addition
		document.getElementById("otherType_field").innerHTML = "<input type=\"text\" id=\"" + sfFieldVal + "\" name=\"" + sfFieldVal + "\" value=\"\" class=\"inputbox_search\" size=\"30\" style=\"border: 1px solid #95a22c; margin-top:8px;\">";
	} else {
		// hide otherRole objects
		document.getElementById("otherType_field").innerHTML = "<input type=\"hidden\" id=\"" + sfFieldVal + "\" name=\"" + sfFieldVal + "\" value=\"" + currTypeVal + "\">";
	}
}


function nocookiewarn(warntext1,warntext2) {
	document.write("<span class='required_asterix'>" + warntext1 + " <img src='http://www.pentaho.com/images/lock_com.png' alt='' />. " + warntext2 + " <a href='http://www.google.com/cookies.html' target='_blank'>http://www.google.com/cookies.html</a>.</span>");
}


/*
collect form data and pass thru image tag
faction - form action value
formname - name/id of form we are submitting
*/
function handleForm(faction,formname) {
	var checkForm = CheckRequiredFields();
	if(checkForm) {
		var newimg;
		newimg = document.createElement("img");
		newimg.id = "formsub";
		newimg.style.display = "none";
			var webForm = document.forms[formname];
			var subURL = "";
			var objTmp = "";
			for(var x = 0; x < webForm.length; ++x) {
				objTmp = webForm[x].value;
				objTmp = objTmp.replace(/[\(\)\<\>\;\\\"\[\]]/g,'');
				subURL += webForm[x].name + "=" + objTmp + "&";
				objTmp = "";
			}
		newimg.src = faction + "?" + subURL;
		document.body.appendChild(newimg);
		return true;
	}
	return false;
}




















